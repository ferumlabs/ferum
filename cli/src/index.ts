#!/usr/bin/env node

import { program } from "commander";
import log from "loglevel";
import util from "util";
import { createTestCoin, getTestCoinBalance} from "./test-coin";
import { initializeFerum, initializeOrderbook, addLimitOrder, addMarketOrder, cancelOrder } from "./market";
import { AptosClient } from "aptos";
import { Transaction_UserTransaction } from "aptos/dist/generated";

export const NODE_URL = "https://fullnode.devnet.aptoslabs.com";

const client = new AptosClient(NODE_URL);

program.version("1.1.0");
log.setLevel("info");

programCommand("create-test-coin")
  .option("-sk, --signer-key <string>", "private key of the test coin module account.", process.env.APTOS_KEY)
  .option("-m, --module-path <string>", "coin module path.")
  .option("-n, --coin-name <string>", "coin name.")
  .action(async (_, cmd) => {
    const { signerKey, modulePath, coinName} = cmd.opts();
    await createTestCoin(signerKey, modulePath, coinName)
  });

programCommand("test-coin-balances")
  .option("-sk, --signer-key <string>", "private key of the test coin module account.", process.env.APTOS_KEY)
  .action(async (_, cmd) => {
    const { signerKey } = cmd.opts();
    prettyPrint("Coin Balances", {
      "test coins: " : await getTestCoinBalance(signerKey, "test_coin::TestCoin"),
      "berry coins: " :  await getTestCoinBalance(signerKey, "berry_coin::BerryCoin"),
    })
  });

programCommand("init-ferum")
  .option("-sk, --signer-key <string>", "private key of the signer.", process.env.APTOS_KEY)
  .action(async (_, cmd) => {
    const { signerKey } = cmd.opts();
    const txHash = await initializeFerum(signerKey)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(`Completed transaction with success: ${txResult.success}:`, txResult)
  });

programCommand("init-orderbook")
  .option("-sk, --signer-key <string>", "private key of the signer.", process.env.APTOS_KEY)
  .option("-ic, --instrument-coin <string>", "instrument coin.")
  .option("-qc, --quote-coin <string>", "quote coin.")
  .action(async (_, cmd) => {
    const { signerKey, instrumentCoin, quoteCoin } = cmd.opts();
    const txHash = await initializeOrderbook(signerKey, instrumentCoin, quoteCoin)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(`Completed transaction with success: ${txResult.success}:`, txResult)
  });

  programCommand("add-limit-order")
  .option("-sk, --signer-key <string>", "private key of the signer.", process.env.APTOS_KEY)
  .option("-ic, --instrument-coin <string>", "instrument token.")
  .option("-qc, --quote-coin <string>", "quote token.")
  .option("-p, --price <number>", "quote price.")
  .option("-q, --quantity <number>", "quote price.")
  .option("-s, --side <string>", "side: either buy or sell.")
  .action(async (_, cmd) => {
    const { signerKey, instrumentCoin, quoteCoin, price, quantity, side } = cmd.opts();
    const txHash = await addLimitOrder(signerKey, instrumentCoin, quoteCoin, side, price, quantity)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(`Completed transaction with success: ${txResult.success}:`, txResult)
  });

  programCommand("add-market-order")
  .option("-sk, --signer-key <string>", "private key of the signer.", process.env.APTOS_KEY)
  .option("-ic, --instrument-coin <string>", "instrument token.")
  .option("-qc, --quote-coin <string>", "quote token.")
  .option("-s, --side <string>", "side: either buy or sell.")
  .option("-q, --quantity <number>", "quote price.")
  .option("-c, --max-collateral <number>", "max collateral.")
  .action(async (_, cmd) => {
    const { signerKey, instrumentCoin, quoteCoin, side, quantity, maxCollateral } = cmd.opts();
    const txHash = await addMarketOrder(signerKey, instrumentCoin, quoteCoin, side, quantity, maxCollateral)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(`Completed transaction with success: ${txResult.success}:`, txResult)
  });


  programCommand("cancel-order")
  .option("-sk, --signer-key <string>", "private key of the signer.", process.env.APTOS_KEY)
  .option("-id, --order-id <number>", "order id.")
  .action(async (_, cmd) => {
    const { signerKey, orderID } = cmd.opts();
    const txHash = await cancelOrder(signerKey, orderID)
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