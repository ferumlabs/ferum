import axios from 'axios';
import path from 'path';
import BN from 'bignumber.js';
import { addOrder, cancelOrder } from '../market';
import { AptosAccount, Types } from 'aptos';
import { getClient } from '../aptos-client';
import Config from '../config';


Config.setEnv('testnet');

const InstrumentType = '0x1::aptos_coin::AptosCoin';
const InstrumentDecimals = 4;
const QuoteType = '0xc4a97809df332af8bb20ebe1c60f47b7121648c5896f29dc37b4d2e60944e20d::test_coins::USDF';
const QuoteDecimals = 4;

async function getPrice(instrumentType: string, quoteType: string) {
  const url = path.join('https://api.ferum.xyz/', 'price', instrumentType, quoteType); 
  const data = (await axios.get(url)).data;

  const quote = data['quote'];
  const maxBid = new BN(quote['max_bid']);
  const minAsk = new BN(quote['min_ask'])
  return [maxBid.plus(minAsk).div(2), maxBid, minAsk];
}

async function main() {
  const account = new AptosAccount(Uint8Array.from(Buffer.from('<PKEY>', 'hex')));

  let inProgress = false;
  let incr = new BN('0.1');
  setInterval(async () => {
    if (inProgress) return;
    inProgress = true;
    let [midpoint, maxBid, minAsk] = await getPrice(InstrumentType, QuoteType); // Get current price.

    const targetPrice = midpoint.plus(incr);
    if (targetPrice.lt('0.5') || targetPrice.gt('1.5')) {
      incr = incr.times(-1);
    }

    console.log('Current price', midpoint.toFormat(), maxBid.toFormat(), minAsk.toFormat(), targetPrice.toFormat());

    const ask = FP(new BN(5));
    const bid = FP(targetPrice.minus('0.01'));

    await waitForSuccess(async () => {
      return await addOrder(
        account,
        InstrumentType,
        QuoteType,
        'buy',
        'resting',
        bid,
        50000,
        {
          maxGas: 1676500,
        },
      );
    });
    await waitForSuccess(async () => {
      return await addOrder(
        account,
        InstrumentType,
        QuoteType,
        'sell',
        'resting',
        ask,
        10000,
        {
          maxGas: 1676500,
        },
      );
    });

    inProgress = false;
  }, 5000);
}

async function waitForSuccess(fn: () => Promise<string>) {
  const txHash = await fn();
  console.log(txHash);
  const txResult = (await getClient().waitForTransactionWithResult(txHash)) as Types.UserTransaction;
  // console.log(txResult);
  if (!txResult.success) {  
    throw new Error(JSON.stringify(txResult, undefined, 2));
  }
  return;
}

function FP(bn: BN): number {
  return bn.times(new BN(10).pow(QuoteDecimals)).decimalPlaces(0).toNumber();
}



main();


