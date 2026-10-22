// Run this ONCE on your own computer. It makes a brand-new wallet just for the bot.
import { Keypair } from '@solana/web3.js';
import bs58 from 'bs58';

const kp = Keypair.generate();
console.log('\nBOT WALLET ADDRESS (send SOL here from your Jupiter wallet):');
console.log(kp.publicKey.toBase58());
console.log('\nPRIVATE KEY (goes ONLY in .env or a GitHub secret. Never share or commit it):');
console.log(bs58.encode(kp.secretKey));
console.log('');
