#!/usr/bin/env node

import { program } from "commander";
import log from "loglevel";
import util from "util";
import { createTestCoin, getTestCoinBalance, TestCoinSymbol, TEST_COINS } from "./test-coins";
import { initializeFerum, initializeOrderbook, addLimitOrder, addMarketOrder, cancelOrder } from "./market";
import { AptosAccount } from "aptos";
import { Transaction_UserTransaction } from "aptos/dist/generated";
import { publishModuleUsingCLI } from "./utils/module-publish-utils";
import { client, NODE_URL } from './aptos-client';

program.version("1.1.0");
log.setLevel("info");

programCommand("get-address")
  .option("-pk, --private-key <string>", "private key of the account.", process.env.APTOS_KEY)
  .action(async (_, cmd) => {
    const { privateKey } = cmd.opts();
    const privateKeyHex = Uint8Array.from(Buffer.from(privateKey, "hex"));
    const account = new AptosAccount(privateKeyHex);

    console.log(`Address: ${account.address()}`);
  });

programCommand("publish-modules")
  .option("-pk, --private-key <string>", "private key of the module account.", process.env.APTOS_KEY)
  .option("-m, --module-path <string>", "coin module path.")
  .option("-m, --max-gas [number]", "max gas used for transaction.", "10000")
  .action(async (_, cmd) => {
    const { privateKey, modulePath, maxGas } = cmd.opts();

    if (Number(maxGas) === NaN) {
      throw new Error('Invalid max-gas param');
    }

    const privateKeyHex = Uint8Array.from(Buffer.from(privateKey, "hex"));
    const account = new AptosAccount(privateKeyHex);

    console.log('Publishing modules under account', account.address());

    try {
      await publishModuleUsingCLI(NODE_URL, account, modulePath, maxGas);
    } 
    catch {
      console.error('Unable to publish module.');
    }
  });
 
programCommand("create-test-coins")
  .option("-pk, --private-key <string>", "private key of the module account.", process.env.APTOS_KEY)
  .action(async (_, cmd) => {
    const { privateKey } = cmd.opts();
    await createTestCoin(privateKey, "FMA");
    await createTestCoin(privateKey, "FMB");
  });

programCommand("test-coin-balances")
  .option("-pk, --private-key <string>", "private key of the test coin module account.", process.env.APTOS_KEY)
  .action(async (_, cmd) => {
    const { privateKey } = cmd.opts();

    const balances: {[key: string]: number} = {};
    for (let coinSymbol in TEST_COINS) {
      balances[coinSymbol] = await getTestCoinBalance(privateKey, coinSymbol as TestCoinSymbol);
    }

    prettyPrint("Coin Balances", balances);
  });

programCommand("init-ferum")
  .option("-pk, --private-key <string>", "private key of the signer.", process.env.APTOS_KEY)
  .action(async (_, cmd) => {
    const { privateKey } = cmd.opts();
    const txHash = await initializeFerum(privateKey)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(`Completed transaction with success: ${txResult.success}:`, txResult)
  });

programCommand("init-orderbook")
  .option("-pk, --private-key <string>", "private key of the signer.", process.env.APTOS_KEY)
  .option("-ic, --instrument-coin <string>", "instrument coin.")
  .option("-qc, --quote-coin <string>", "quote coin.")
  .action(async (_, cmd) => {
    const { privateKey, instrumentCoin, quoteCoin } = cmd.opts();
    const txHash = await initializeOrderbook(privateKey, instrumentCoin, quoteCoin)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(`Completed transaction with success: ${txResult.success}:`, txResult)
  });

  programCommand("add-limit-order")
  .option("-pk, --private-key <string>", "private key of the signer.", process.env.APTOS_KEY)
  .option("-ic, --instrument-coin <string>", "instrument token.")
  .option("-qc, --quote-coin <string>", "quote token.")
  .option("-p, --price <number>", "quote price.")
  .option("-q, --quantity <number>", "quote price.")
  .option("-s, --side <string>", "side: either buy or sell.")
  .action(async (_, cmd) => {
    const { privateKey, instrumentCoin, quoteCoin, price, quantity, side } = cmd.opts();
    const txHash = await addLimitOrder(privateKey, instrumentCoin, quoteCoin, side, price, quantity)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(`Completed transaction with success: ${txResult.success}:`, txResult)
  });

  programCommand("add-market-order")
  .option("-pk, --private-key <string>", "private key of the signer.", process.env.APTOS_KEY)
  .option("-ic, --instrument-coin <string>", "instrument token.")
  .option("-qc, --quote-coin <string>", "quote token.")
  .option("-s, --side <string>", "side: either buy or sell.")
  .option("-q, --quantity <number>", "quote price.")
  .option("-c, --max-collateral <number>", "max collateral.")
  .action(async (_, cmd) => {
    const { privateKey, instrumentCoin, quoteCoin, side, quantity, maxCollateral } = cmd.opts();
    const txHash = await addMarketOrder(privateKey, instrumentCoin, quoteCoin, side, quantity, maxCollateral)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(`Completed transaction with success: ${txResult.success}:`, txResult)
  });


  programCommand("cancel-order")
  .option("-pk, --private-key <string>", "private key of the signer.", process.env.APTOS_KEY)
  .option("-id, --order-id <number>", "order id.")
  .action(async (_, cmd) => {
    const { privateKey, orderID } = cmd.opts();
    const txHash = await cancelOrder(privateKey, orderID)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(`Completed transaction with success: ${txResult.success}:`, txResult)
  });

function programCommand(name: string) {
  return program
    .command(name)
    .option("-l, --log-level <string>", "log level", setLogLevel);
}

// eslint-disable-next-line @typescript-eslint/no-unused-vars
function setLogLevel(value: any) {
  if (value === undefined || value === null) {
    return;
  }
  log.info("setting the log value to: " + value);
  log.setLevel(value);
}

function prettyPrint(description: string, obj: any) {
  log.info(description);
  log.info(util.inspect(obj, { colors: true, depth: 6 }));
}

program.parseAsync(process.argv);