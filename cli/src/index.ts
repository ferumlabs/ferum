#!/usr/bin/env node

import { program } from "commander";
import log from "loglevel";
import util from "util";
import { createTestCoin, getTestCoinBalance, TestCoinSymbol, TEST_COINS } from "./test-coins";
import { initializeFerum, initializeMarket, addLimitOrder, addMarketOrder, cancelOrder } from "./market";
import { AptosAccount } from "aptos";
import { Transaction_UserTransaction } from "aptos/dist/generated";
import { publishModuleUsingCLI } from "./utils/module-publish-utils";
import { client, NODE_URL } from './aptos-client';

program.version("1.1.0");
log.setLevel("info");

programCommand("get-address")
  .option("-pk, --private-key [string]", "private key of the account.", process.env.APTOS_KEY)
  .action(async (_, cmd) => {
    const { privateKey } = cmd.opts();
    const privateKeyHex = Uint8Array.from(Buffer.from(privateKey, "hex"));
    const account = new AptosAccount(privateKeyHex);

    console.log(`Address: ${account.address()}`);
  });

programCommand("publish-modules")
  .option("-pk, --private-key [string]", "private key of the module account.", process.env.APTOS_KEY)
  .option("-m, --module-path <string>", "coin module path.")
  .option("-m, --max-gas [number]", "max gas used for transaction.", "10000")
  .action(async (_, cmd) => {
    const { privateKey, modulePath, maxGas } = cmd.opts();

    const maxGasNum = parseNumber(maxGas, 'max-gas');

    const privateKeyHex = Uint8Array.from(Buffer.from(privateKey, "hex"));
    const account = new AptosAccount(privateKeyHex);

    console.log('Publishing modules under account', account.address());

    try {
      await publishModuleUsingCLI(NODE_URL, account, modulePath, maxGasNum);
    } 
    catch {
      console.error('Unable to publish module.');
    }
  });
 
programCommand("create-test-coins")
  .option("-pk, --private-key [string]", "private key of the module account.", process.env.APTOS_KEY)
  .action(async (_, cmd) => {
    const { privateKey } = cmd.opts();
    await createTestCoin(privateKey, "FMA");
    await createTestCoin(privateKey, "FMB");
  });

programCommand("test-coin-balances")
  .option("-pk, --private-key [string]", "private key of the test coin module account.", process.env.APTOS_KEY)
  .action(async (_, cmd) => {
    const { privateKey } = cmd.opts();

    const balances: {[key: string]: number} = {};
    for (let coinSymbol in TEST_COINS) {
      balances[coinSymbol] = await getTestCoinBalance(privateKey, coinSymbol as TestCoinSymbol);
    }

    prettyPrint("Coin Balances", balances);
  });

programCommand("init-ferum")
  .option("-pk, --private-key [string]", "private key of the signer.", process.env.APTOS_KEY)
  .action(async (_, cmd) => {
    const { privateKey } = cmd.opts();
    const txHash = await initializeFerum(privateKey)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(transactionStatusMessage(txResult), txResult)
  });

programCommand("init-market")
  .option("-pk, --private-key [string]", "private key of the signer.", process.env.APTOS_KEY)
  .option(
    "-ic, --instrument-coin-type <string>", 
    "instrument coin type. can be a symbol if identifying a test coin. \
    Other wise, must be a fully qualified coin type name (addrss::module::CoinType)."
  )
  .option(
    "-id, --instrument-decimals <number>", 
    "decimal places for the instrument coin. Must be <= the underlying coin's decimals. \
    The sum of the instrument and quote decimals must also be <= min(coin::decimals(Instrument), coin::decimals(Quote))"
  )
  .option(
    "-qc, --quote-coin-type <string>", 
    "quote coin type. can be a symbol if identifying a test coin. \
    Other wise, must be a fully qualified coin type name (addrss::module::CoinType)."
  )
  .option(
    "-qd, --quote-decimals <number>", 
    "decimal places for the quote coin. Must be <= the underlying coin's decimals. \
    The sum of the instrument and quote decimals must also be <= min(coin::decimals(Instrument), coin::decimals(Quote))"
  )
  .action(async (_, cmd) => {
    const { privateKey, instrumentCoinType, quoteCoinType, quoteDecimals, instrumentDecimals } = cmd.opts();

    const instrumentDecimalsNum = parseNumber(instrumentDecimals, 'instrument-decimals');
    const quoteDecimalsNum = parseNumber(quoteDecimals, 'quote-decimals');

    const txHash = await initializeMarket(privateKey, instrumentCoinType, instrumentDecimalsNum, quoteCoinType, quoteDecimalsNum);
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(transactionStatusMessage(txResult), txResult)
  });

  programCommand("add-limit-order")
  .option("-pk, --private-key [string]", "private key of the signer.", process.env.APTOS_KEY)
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
    prettyPrint(transactionStatusMessage(txResult), txResult)
  });

  programCommand("add-market-order")
  .option("-pk, --private-key [string]", "private key of the signer.", process.env.APTOS_KEY)
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
    prettyPrint(transactionStatusMessage(txResult), txResult)
  });


  programCommand("cancel-order")
  .option("-pk, --private-key [string]", "private key of the signer.", process.env.APTOS_KEY)
  .option("-id, --order-id <number>", "order id.")
  .action(async (_, cmd) => {
    const { privateKey, orderID } = cmd.opts();
    const txHash = await cancelOrder(privateKey, orderID)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(transactionStatusMessage(txResult), txResult)
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

function transactionStatusMessage(txResult: Transaction_UserTransaction) {
  return txResult.success ? (
    'Transaction Succeded'
  ) : (
    'Transaction Failed'
  );
}

function parseNumber(n: any, paramName: string): number {
  const out = Number(n);
  if (Number.isNaN(out)) {
    throw new Error(`Invalid number for ${paramName} param`);
  }
  return out;
}

program.parseAsync(process.argv);