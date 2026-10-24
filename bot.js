// Solana pump.fun scalper, built to run as short sessions on GitHub Actions.
// Each run: sweep leftovers -> trade for SESSION_SECONDS -> sell everything -> reclaim rent -> exit.
import 'dotenv/config';
import WebSocket from 'ws';
import bs58 from 'bs58';
import { Connection, Keypair, PublicKey, Transaction, VersionedTransaction } from '@solana/web3.js';
import { TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID, createCloseAccountInstruction } from '@solana/spl-token';

const env = (k, d) => (process.env[k] !== undefined && process.env[k] !== '' ? process.env[k] : d);
const num = (k, d) => Number(env(k, String(d)));

const C = {
  DRY_RUN: env('DRY_RUN', 'true') !== 'false', // true = pretend trades, nothing is sent
  TRADE_SOL: num('TRADE_SOL', 0.002),          // size of each buy, in SOL
  MIN_BALANCE_SOL: num('MIN_BALANCE_SOL', 0),  // loss cap: if wallet SOL is below this, bot does not trade
  PRIORITY_FEE_SOL: num('PRIORITY_FEE_SOL', 0.00005),
  BUY_SLIPPAGE: num('BUY_SLIPPAGE', 25),       // percent
  SELL_SLIPPAGE: num('SELL_SLIPPAGE', 40),     // percent
  SESSION_SECONDS: num('SESSION_SECONDS', 210),
  NO_ENTRY_LAST_SECONDS: num('NO_ENTRY_LAST_SECONDS', 60),

  // entry filters (tune after you see real results)
  MIN_OBSERVE_SECONDS: num('MIN_OBSERVE_SECONDS', 5),
  MAX_OBSERVE_SECONDS: num('MAX_OBSERVE_SECONDS', 40),
  POLL_MS: num('POLL_MS', 1500),                  // how often to read bonding curves from the chain
  MIN_INFLOW_SOL: num('MIN_INFLOW_SOL', 1.5),     // net SOL that flowed into the curve since we started watching
  MAX_SINGLE_DROP: num('MAX_SINGLE_DROP', 0.10),  // skip tokens that dumped >10% in one poll while watched
  DUMP_DROP: num('DUMP_DROP', 0.15),              // sell all if price drops >15% between two polls
  MAX_DEV_BUY_SOL: num('MAX_DEV_BUY_SOL', 1.5),
  MAX_MCAP_SOL: num('MAX_MCAP_SOL', 38),
  MAX_PRICE_RISE: num('MAX_PRICE_RISE', 0.3),     // skip if already up more than 60% since launch

  // exits
  STOP_LOSS: num('STOP_LOSS', 0.25),              // sell all at -25% (before first take-profit)
  TIME_STOP_SECONDS: num('TIME_STOP_SECONDS', 90),
  TP1: num('TP1', 1.5), TP1_SELL_PCT: num('TP1_SELL_PCT', 40), // sell 40% of position at 1.5x
  TP2: num('TP2', 2.0), TP2_SELL_PCT: num('TP2_SELL_PCT', 30), // sell 30% of original at 2x
  TP3: num('TP3', 3.0),                                        // sell the rest at 3x
  TRAIL: num('TRAIL', 0.2),                                    // after TP1, sell rest if down 20% from peak
  STALE_SECONDS: num('STALE_SECONDS', 25),
};

if (!process.env.BOT_WALLET_PRIVATE_KEY) {
  console.error('Missing BOT_WALLET_PRIVATE_KEY');
  process.exit(1);
}
const wallet = Keypair.fromSecretKey(bs58.decode(process.env.BOT_WALLET_PRIVATE_KEY));
const conn = new Connection(env('HELIUS_RPC_URL', 'https://api.mainnet-beta.solana.com'), 'confirmed');

const now = () => Date.now();
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const log = (...a) => console.log(new Date().toISOString().slice(11, 19), ...a);

let endAt = now() + C.SESSION_SECONDS * 1000;
let ending = false;
let ws;
const watch = new Map(); // mint -> token being observed
let dbg = 0;
let polling = false;
const stats = { created: 0, devBig: 0, polls: 0, maxInflow: 0, maxRise: 0, entered: 0 };
let pos = null;          // the one open position
let entering = false;

// ---------- trading ----------
async function confirm(sig, ms) {
  const until = now() + ms;
  while (now() < until) {
    try {
      const { value } = await conn.getSignatureStatuses([sig]);
      const s = value[0];
      if (s) {
        if (s.err) return false;
        if (s.confirmationStatus === 'confirmed' || s.confirmationStatus === 'finalized') return true;
      }
    } catch {}
    await sleep(700);
  }
  return false;
}

async function trade(action, mint, amount, denominatedInSol, slippage, pool, confirmMs = 20000) {
  try {
    const res = await fetch('https://pumpportal.fun/api/trade-local', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        publicKey: wallet.publicKey.toBase58(),
        action,
        mint,
        amount,
        denominatedInSol: String(denominatedInSol),
        slippage,
        priorityFee: C.PRIORITY_FEE_SOL,
        pool,
      }),
    });
    if (res.status !== 200) {
      log(`${action} API error ${res.status}: ${(await res.text()).slice(0, 120)}`);
      return null;
    }
    const tx = VersionedTransaction.deserialize(new Uint8Array(await res.arrayBuffer()));
    tx.sign([wallet]);
    const sig = await conn.sendRawTransaction(tx.serialize(), { skipPreflight: true, maxRetries: 3 });
    const ok = await confirm(sig, confirmMs);
    if (!ok) log(`${action} not confirmed: ${sig}`);
    return ok ? sig : null;
  } catch (e) {
    log(`${action} failed: ${e.message}`);
    return null;
  }
}

async function heldTokens() {
  const found = [];
  for (const programId of [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID]) {
    const r = await conn.getParsedTokenAccountsByOwner(wallet.publicKey, { programId });
    for (const a of r.value) {
      const i = a.account.data.parsed.info;
      if (Number(i.tokenAmount.amount) > 0) found.push(i.mint);
    }
  }
  return found;
}

// Sell any leftover tokens (from a failed exit in a previous run). Bot wallet only!
async function sweepHoldings() {
  if (C.DRY_RUN) return;
  try {
    const mints = await heldTokens();
    for (const mint of mints.slice(0, 4)) {
      log('sweeping leftover', mint);
      await trade('sell', mint, '100%', false, 80, 'auto', 8000);
    }
  } catch (e) {
    log('sweep error', e.message);
  }
}

// Close empty token accounts to get the ~0.002 SOL rent back (matters a lot at tiny sizes).
async function reclaimRent() {
  if (C.DRY_RUN || pos || entering) return;
  try {
    for (const programId of [TOKEN_PROGRAM_ID, TOKEN_2022_PROGRAM_ID]) {
      const r = await conn.getParsedTokenAccountsByOwner(wallet.publicKey, { programId });
      const empty = r.value.filter((a) => a.account.data.parsed.info.tokenAmount.amount === '0');
      for (let i = 0; i < empty.length; i += 8) {
        const tx = new Transaction();
        for (const a of empty.slice(i, i + 8)) {
          tx.add(createCloseAccountInstruction(a.pubkey, wallet.publicKey, wallet.publicKey, [], programId));
        }
        tx.feePayer = wallet.publicKey;
        tx.recentBlockhash = (await conn.getLatestBlockhash()).blockhash;
        tx.sign(wallet);
        await conn.sendRawTransaction(tx.serialize());
      }
      if (empty.length) log(`reclaiming rent from ${empty.length} empty accounts`);
    }
  } catch (e) {
    log('reclaim error', e.message);
  }
}

// ---------- market data ----------
// PumpPortal's trade feed needs a paid key, so we only use the free new-token feed
// and read each token's bonding curve directly from the chain (via your RPC).
const PUMP_PROGRAM = new PublicKey('6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P');
const curveAddress = (mint) =>
  PublicKey.findProgramAddressSync([Buffer.from('bonding-curve'), new PublicKey(mint).toBuffer()], PUMP_PROGRAM)[0];

function parseCurve(data) {
  if (!data || data.length < 49) return null;
  const b = Buffer.from(data);
  const vTok = b.readBigUInt64LE(8);   // virtual token reserves
  const vSol = b.readBigUInt64LE(16);  // virtual SOL reserves
  const rSol = b.readBigUInt64LE(32);  // real SOL reserves (net SOL in the curve)
  const complete = b[48] === 1;        // migrated off the curve
  if (vTok === 0n) return null;
  return { price: Number(vSol) / 1e9 / (Number(vTok) / 1e6), realSol: Number(rSol) / 1e9, complete };
}

function connectWs() {
  ws = new WebSocket('wss://pumpportal.fun/api/data');
  ws.on('open', () => {
    log('ws connected');
    ws.send(JSON.stringify({ method: 'subscribeNewToken' }));
  });
  ws.on('message', (raw) => {
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    if (dbg < 3 && m.txType !== 'create') { dbg++; log('raw', raw.toString().slice(0, 200)); }
    onCreate(m);
  });
  ws.on('close', () => { if (!ending) setTimeout(connectWs, 1000); });
  ws.on('error', (e) => log('ws error', e.message));
}

function onCreate(m) {
  if (m.txType !== 'create' || !m.mint) return;
  const s0 = Number(m.vSolInBondingCurve), t0 = Number(m.vTokensInBondingCurve);
  if (!(s0 > 0 && t0 > 0)) return;
  stats.created++;
  if (watch.size >= 40) return;
  if (Number(m.solAmount || 0) > C.MAX_DEV_BUY_SOL) { stats.devBig++; return; }
  let bc;
  try { bc = curveAddress(m.mint); } catch { return; }
  const price = s0 / t0;
  watch.set(m.mint, {
    mint: m.mint, bc, dev: m.traderPublicKey, name: m.symbol || m.name || m.mint.slice(0, 6),
    createdAt: now(), startPrice: price, price, mcap: price * 1e9,
    baseSol: null, baseAt: 0, realSol: 0, maxDrop: 0, complete: false, samples: [],
  });
}

async function poll() {
  if (polling || ending) return;
  polling = true;
  try {
    const items = [...watch.values()];
    if (!items.length) return;
    const infos = await conn.getMultipleAccountsInfo(items.map((w) => w.bc));
    stats.polls++;
    infos.forEach((info, i) => {
      const c = info && parseCurve(info.data);
      if (c) onCurve(items[i], c);
    });
  } catch (e) {
    log('poll error', e.message);
  } finally {
    polling = false;
  }
}

function onCurve(w, c) {
  const t = now();
  const drop = 1 - c.price / w.price; // positive = price fell since last poll
  w.price = c.price;
  w.mcap = c.price * 1e9;
  w.realSol = c.realSol;
  w.complete = c.complete;
  if (w.baseSol === null) { w.baseSol = c.realSol; w.baseAt = t; }
  if (drop > w.maxDrop) w.maxDrop = drop;
  w.samples.push(c.price);
  if (w.samples.length > 4) w.samples.shift();

  if (pos && pos.mint === w.mint) {
    pos.price = c.price;
    pos.peak = Math.max(pos.peak, c.price);
    pos.lastEventAt = t;
    if (drop >= C.DUMP_DROP) pos.dumped = true;
    return;
  }
  stats.maxInflow = Math.max(stats.maxInflow, c.realSol - w.baseSol);
  stats.maxRise = Math.max(stats.maxRise, c.price / w.startPrice - 1);
  maybeEnter(w);
}

// ---------- entry ----------
function maybeEnter(w) {
  if (pos || entering || ending) return;
  if (now() > endAt - C.NO_ENTRY_LAST_SECONDS * 1000) return;
  const age = (now() - w.createdAt) / 1000;
  if (age < C.MIN_OBSERVE_SECONDS || age > C.MAX_OBSERVE_SECONDS) return;
  if (w.complete || w.baseSol === null) return;
  if (w.maxDrop > C.MAX_SINGLE_DROP) return;                       // had a sharp dump
  if (w.realSol - w.baseSol < C.MIN_INFLOW_SOL) return;            // not enough net buying
  if (w.mcap > C.MAX_MCAP_SOL) return;
  const rise = w.price / w.startPrice - 1;
  if (rise < 0 || rise > C.MAX_PRICE_RISE) return;
  const n = w.samples.length;
  if (n >= 3 && w.samples[n - 1] < w.samples[n - 3]) return;       // momentum fading
  enter(w);
}

async function enter(w) {
  entering = true;
  stats.entered++;
  try {
    const entryPrice = w.price;
    log(`ENTER ${w.name} ${w.mint} inflow=${(w.realSol - w.baseSol).toFixed(2)}SOL mcap=${w.mcap.toFixed(1)} rise=${((w.price / w.startPrice - 1) * 100).toFixed(0)}%`);
    if (!C.DRY_RUN) {
      const sig = await trade('buy', w.mint, C.TRADE_SOL, true, C.BUY_SLIPPAGE, 'pump');
      if (!sig) {
        log('buy failed, skipping token');
        watch.delete(w.mint);
        return;
      }
    }
    pos = {
      mint: w.mint, name: w.name, dev: w.dev, entry: entryPrice, price: w.price, peak: Math.max(entryPrice, w.price), dumped: false,
      enteredAt: now(), lastEventAt: now(), stage: 0, remainingPct: 100, realized: 0,
      selling: false, sellFails: 0,
    };
  } finally {
    entering = false;
  }
}

// ---------- exits ----------
async function sell(pct, reason, nextStage) {
  const p = pos;
  p.selling = true;
  const mult = p.price / p.entry;
  log(`SELL ${pct}% ${p.name} reason=${reason} at ${mult.toFixed(2)}x`);
  let ok = true;
  if (!C.DRY_RUN) {
    const slip = p.sellFails > 0 ? 80 : C.SELL_SLIPPAGE;
    ok = !!(await trade('sell', p.mint, `${pct}%`, false, slip, 'auto'));
  }
  if (!ok) {
    p.sellFails++;
    p.selling = false;
    log(`sell failed (${p.sellFails}), will retry`);
    if (p.sellFails >= 6) {
      log(`giving up on ${p.name}, leftover tokens will be swept next run`);
      watch.delete(p.mint);
      pos = null;
    }
    return;
  }
  const frac = (p.remainingPct * pct) / 100;
  p.realized += (frac / 100) * mult;
  p.remainingPct -= frac;
  if (nextStage) p.stage = nextStage;
  p.selling = false;
  if (pct >= 100 || p.remainingPct <= 0.5) {
    log(`CLOSED ${p.name} estimated result ${p.realized.toFixed(2)}x (before fees)`);
    watch.delete(p.mint);
    pos = null;
    reclaimRent();
  }
}

async function tick() {
  for (const [mint, w] of watch) {
    if ((now() - w.createdAt) / 1000 > C.MAX_OBSERVE_SECONDS + 2 && !(pos && pos.mint === mint) && !(entering)) {
      watch.delete(mint);
    }
  }
  if (!pos || pos.selling || entering) return;

  const mult = pos.price / pos.entry;
  const heldSec = (now() - pos.enteredAt) / 1000;
  let reason = null;
  if (pos.dumped) reason = 'dump detected';
  else if (now() >= endAt) reason = 'session end';
  else if (pos.stage === 0 && mult <= 1 - C.STOP_LOSS) reason = 'stop loss';
  else if (pos.stage === 0 && heldSec > C.TIME_STOP_SECONDS && mult < 1.3) reason = 'time stop';
  else if ((now() - pos.lastEventAt) / 1000 > C.STALE_SECONDS) reason = 'no price updates';
  else if (mult >= C.TP3) reason = 'tp3';
  else if (pos.stage >= 1 && pos.price <= pos.peak * (1 - C.TRAIL)) reason = 'trailing stop';
  if (reason) return sell(100, reason);

  if (pos.stage === 0 && mult >= C.TP1) return sell(C.TP1_SELL_PCT, 'tp1', 1);
  if (pos.stage === 1 && mult >= C.TP2) {
    const pct = Math.min(100, Math.round((C.TP2_SELL_PCT / pos.remainingPct) * 100));
    return sell(pct, 'tp2', 2);
  }
}

// ---------- main ----------
async function main() {
  log(`start | dry_run=${C.DRY_RUN} | trade=${C.TRADE_SOL} SOL | wallet=${wallet.publicKey.toBase58().slice(0, 6)}...`);
  await sweepHoldings();
  await reclaimRent();

  const bal = (await conn.getBalance(wallet.publicKey)) / 1e9;
  log(`balance ${bal.toFixed(4)} SOL`);
  if (C.MIN_BALANCE_SOL > 0 && bal < C.MIN_BALANCE_SOL) {
    log(`below loss-cap floor (${C.MIN_BALANCE_SOL} SOL), not trading`);
    return;
  }
  if (!C.DRY_RUN && bal < C.TRADE_SOL + 0.004) {
    log('balance too low for a trade plus fees/rent, not trading');
    return;
  }

  endAt = now() + C.SESSION_SECONDS * 1000;
  connectWs();
  const pollTimer = setInterval(poll, C.POLL_MS);

  await new Promise((resolve) => {
    const iv = setInterval(async () => {
      try { await tick(); } catch (e) { log('tick error', e.message); }
      const done = now() >= endAt && !pos && !entering;
      const forced = now() > endAt + 45000;
      if (done || forced) { clearInterval(iv); resolve(); }
    }, 500);
  });

  ending = true;
  clearInterval(pollTimer);
  try { ws.close(); } catch {}
  await sweepHoldings();
  await reclaimRent();
  const end = (await conn.getBalance(wallet.publicKey)) / 1e9;
  log('stats', JSON.stringify(stats));
  log(`done | balance ${end.toFixed(4)} SOL (started ${bal.toFixed(4)})`);
}

process.on('unhandledRejection', (e) => log('unhandled', e?.message || e));
main().then(() => process.exit(0)).catch((e) => { console.error(e); process.exit(1); });
