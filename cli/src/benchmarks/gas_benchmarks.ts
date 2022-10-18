import {AptosAccount, TxnBuilderTypes} from 'aptos';
import log from "loglevel";

import { getClient, getFaucetClient, getNodeURL } from '../aptos-client';
import { addOrder, initializeFerum, initializeMarket, addOrderTxnPayload } from '../market';
import {initializeUSDF, mintUSDF, registerUSDF} from '../usdf';
import { publishModuleUsingCLI } from "../utils/module-publish-utils";
import {sendSignedTransactionWithAccount, simulateTransactionWithAccount} from '../utils/transaction-utils';
import Config from '../config';

log.setLevel("info");

Config.setEnv('local');

const ORDER_COUNT = 10;

async function main() {
  // First, create a new account to run the test on.
  const account = new AptosAccount();
  log.info(`Creating account ${account.address().toString()} and funding with 1,000,000 APT`);

  let txHash = (await getFaucetClient().fundAccount(account.address(), 100000000000000))[0];
  await getClient().waitForTransaction(txHash, {checkSuccess: true});
  log.info(`Account is funded`);

  // Set account as local profile.
  Config.importExistingProfile("local", account.toPrivateKeyObject().privateKeyHex);

  // Deploy Ferum to that account.
  log.info('Publishing modules under account', account.address().toString());
  await publishModuleUsingCLI(Config.getEnv(), "ferum", getNodeURL(), account, './contract', 2000000);
  log.info('Published');

  // Create and mint USDF coin.
  log.info('Creating and minting USDF');
  txHash = await initializeUSDF(account)
  await getClient().waitForTransaction(txHash, {checkSuccess: true});
  txHash = await registerUSDF(account);
  await getClient().waitForTransaction(txHash, {checkSuccess: true});
  txHash = await mintUSDF(account, 1000000000000);
  await getClient().waitForTransaction(txHash, {checkSuccess: true});

  // Initialize Ferum.
  log.info('Initing Ferum');
  txHash = await initializeFerum(account);
  await getClient().waitForTransaction(txHash, {checkSuccess: true});

  // Initialize market.
  log.info('Initing Market');
  txHash = await initializeMarket(
    account,
    '0x1::aptos_coin::AptosCoin', 4,
    `${account.address()}::test_coins::USDF`, 4,
  );
  await getClient().waitForTransaction(txHash, {checkSuccess: true});

  // Submit ORDER_COUNT sell and buy limit orders.
  log.info('Submitting Base Orders');
  const promises = [];
  const midPrice = ORDER_COUNT * 10000;
  const {sequence_number: seqNum} = await getClient().getAccount(account.address());
  for (let i = 0; i < ORDER_COUNT; i++) {
    promises.push(new Promise<void>(async resolve => {
      setTimeout(async () => {
        let txHash = await addOrder(
          account,
          '0x1::aptos_coin::AptosCoin',
          `${account.address()}::test_coins::USDF`,
          'sell',
          'resting',
          midPrice + (i + 1),
          1,
          {seqNum: Number(seqNum) + i, maxGas: 20000},
        );
        console.log(txHash);
        await getClient().waitForTransaction(txHash, {checkSuccess: true, timeoutSecs: 120});
        resolve();
      }, i * 100);
    }))
  }
  // for (let i = 0; i < ORDER_COUNT; i++) {
  //   await addOrder(
  //     account,
  //     '0x1::aptos_coin::AptosCoin',
  //     `${account.address()}::test_coins::USDF`,
  //     'buy',
  //     'resting',
  //     midPrice - (i + 1),
  //     1,
  //   );
  //   await getClient().waitForTransaction(txHash, {checkSuccess: true});
  // }
  log.info('Waiting for resolution');
  await Promise.all(promises);

  // Simulate order that will eat the entire sell side of a book.
  log.info('Simulating Order');
  let payload = addOrderTxnPayload(
    account,
    '0x1::aptos_coin::AptosCoin',
    `${account.address()}::test_coins::USDF`,
    'buy',
    'resting',
    midPrice + 2*ORDER_COUNT,
    1,
  )
  await simulateTransactionWithAccount(account, payload, {maxGas: 200000});
}

// main();

async function bench() {
  // First, create a new account to run the test on.
  const account = new AptosAccount();
  log.info(`Creating account ${account.address().toString()} and funding with 1,000,000 APT`);

  let txHash = (await getFaucetClient().fundAccount(account.address(), 100000000000000))[0];
  await getClient().waitForTransaction(txHash, {checkSuccess: true});
  log.info(`Account is funded`);

  // Deploy benchmarking module to that account.
  log.info('Publishing modules under account', account.address().toString());
  await publishModuleUsingCLI(Config.getEnv(), "test", getNodeURL(), account, './benchmarking', 10000);
  log.info('Published');

  // Measure create vec.
  const createVec = TxnBuilderTypes.EntryFunction.natural(
    `${account.address()}::benchmarking`,
    "create_vec",
    [],
    []
  );
  await simulateTransactionWithAccount(account, createVec, {maxGas: 2000000});
  // Actually create the vector.
  txHash = await sendSignedTransactionWithAccount(account, createVec, {maxGas: 2000000})
  await getClient().waitForTransaction(txHash, {checkSuccess: true});

  // Expand vec.
  const expand = TxnBuilderTypes.EntryFunction.natural(
    `${account.address()}::benchmarking`,
    "expand_vec",
    [],
    []
  );
  for (let i = 0; i < 10; i++) {
    txHash = await sendSignedTransactionWithAccount(account, expand, {maxGas: 2000000})
    await getClient().waitForTransaction(txHash, {checkSuccess: true});
  }

  // Simulate cost to add.
  const addTo = TxnBuilderTypes.EntryFunction.natural(
    `${account.address()}::benchmarking`,
    "add_to_vec",
    [],
    []
  );
  await simulateTransactionWithAccount(account, addTo, {maxGas: 2000000})

  //
  //
  // txHash = await initializeFerum(account);
  // await getClient().waitForTransaction(txHash, {checkSuccess: true});

  // simulateTransactionWithAccount(account, payload);
}

bench();
