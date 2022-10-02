import { AptosAccount } from 'aptos';
import log from "loglevel";

import { getClient, getFaucetClient, getNodeURL } from '../aptos-client';
import { addLimitOrder, initializeFerum, initializeMarket, addLimitTxnPayload } from '../market';
import { initializeUSDF, mintUSDF } from '../usdf';
import { setEnv } from '../utils/env';
import { publishModuleUsingCLI } from "../utils/module-publish-utils";
import { simulateTransactionWithAccount } from '../utils/transaction-utils';

log.setLevel("info");

setEnv('devnet');

const ORDER_COUNT = 100;

async function main() {
  // First, create a new account to run the test on.
  const account = new AptosAccount();
  log.info(`Creating account ${account.address().toString()} and funding with 1,000,000 APT`);

  let txHash = (await getFaucetClient().fundAccount(account.address(), 100000000000000))[0];
  await getClient().waitForTransaction(txHash[0], {checkSuccess: true});
  log.info(`Account is funded`);

  // Delploy Ferum to that account.
  log.info('Publishing modules under account', account.address().toString());
  await publishModuleUsingCLI(getNodeURL(), account, './contract', 10000000000000);
  log.info('Published');

  // Create and mint USDF coin.
  log.info('Creating and minting USDF');
  txHash = await initializeUSDF(account)
  await getClient().waitForTransaction(txHash, {checkSuccess: true});
  txHash = await mintUSDF(account, 1000000000000);
  await getClient().waitForTransaction(txHash, {checkSuccess: true});
  
  // Initialize Ferum.
  txHash = await initializeFerum(account);
  await getClient().waitForTransaction(txHash, {checkSuccess: true});

  // Initialize market.
  txHash = await initializeMarket(
    account, 
    '0x1::aptos_coin::AptosCoin', 4, 
    `${account.address()}::test_coins::USDF`, 4,
  );
  await getClient().waitForTransaction(txHash, {checkSuccess: true});

  // Submit ORDER_COUNT sell and buy limit orders.
  const promises = [];
  const midPrice = ORDER_COUNT * 10000;
  for (let i = 0; i < ORDER_COUNT; i++) {
    promises.push(new Promise<void>(async resolve => {
      await addLimitOrder(
        account, 
        '0x1::aptos_coin::AptosCoin',
        `${account.address()}::test_coins::USDF`,
        'sell',
        midPrice + (i + 1),
        1,
      );
      await getClient().waitForTransaction(txHash, {checkSuccess: true});
      resolve();
    }));
  }
  for (let i = 0; i < ORDER_COUNT; i++) {
    promises.push(new Promise<void>(async resolve => {
      await addLimitOrder(
        account, 
        '0x1::aptos_coin::AptosCoin',
        `${account.address()}::test_coins::USDF`,
        'buy',
        midPrice - (i + 1),
        1,
      );
      await getClient().waitForTransaction(txHash, {checkSuccess: true});
      resolve();
    }));
  }
  await Promise.all(promises);
  

  // Simulate order that will eat the entire sell side of a book.
  let payload = addLimitTxnPayload(
    account, 
    '0x1::aptos_coin::AptosCoin',
    `${account.address()}::test_coins::USDF`,
    'buy',
    midPrice + 2*ORDER_COUNT,
    1,
  )
  simulateTransactionWithAccount(account, payload);
}

main();
