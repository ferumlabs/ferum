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

programCommand("publish-ferum")
  .option(
    "-pk, --private-key [string]", 
    "Private key of account ferum should be deployed to. " +
    "Optional. Will use the APTOS_KEY env flag if not provided.", 
    process.env.APTOS_KEY,
  )
  .option("-m, --module-path <string>", "Module path.")
  .option("-g, --max-gas [number]", "Max gas used for transaction. Optional. Defaults to 10000.", "10000")
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
  .description('Create FakeMoneyA (FMA) and FakeMoneyB (FMB) test coins.')
  .option(
    "-pk, --private-key [string]", 
    "Private key of the account signing this transaction. Optional. Will use the APTOS_KEY env flag if not provided.", 
    process.env.APTOS_KEY,
  )
  .action(async (_, cmd) => {
    const { privateKey } = cmd.opts();
    await createTestCoin(privateKey, "FMA");
    await createTestCoin(privateKey, "FMB");
  });

programCommand("test-coin-balances")
  .description('Get FakeMoneyA (FMA) and FakeMoneyB (FMB) balances for the signing account.')
  .option(
    "-pk, --private-key [string]", 
    "Private key of the account signing this transaction. Optional. Will use the APTOS_KEY env flag if not provided.",
    process.env.APTOS_KEY,
  )
  .action(async (_, cmd) => {
    const { privateKey } = cmd.opts();

    const balances: {[key: string]: number} = {};
    for (let coinSymbol in TEST_COINS) {
      balances[coinSymbol] = await getTestCoinBalance(privateKey, coinSymbol as TestCoinSymbol);
    }

    prettyPrint("Coin Balances", balances);
  });

programCommand("init-ferum")
  .option(
    "-pk, --private-key [string]", 
    "Private key of the account signing this transaction. Optional. Will use the APTOS_KEY env flag if not provided.", 
    process.env.APTOS_KEY,
  )
  .action(async (_, cmd) => {
    const { privateKey } = cmd.opts();
    const txHash = await initializeFerum(privateKey)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(transactionStatusMessage(txResult), txResult)
  });

programCommand("init-market")
  .option(
    "-pk, --private-key [string]", 
    "Private key of the account signing this transaction. Optional. Will use the APTOS_KEY env flag if not provided.",
    process.env.APTOS_KEY,
  )
  .option(
    "-ic, --instrument-coin-type <string>", 
    "Instrument CoinType. Can be a symbol if identifying a test coin. " +
    "Otherwise, must be a fully qualified type (address::module::CoinType)."
  )
  .option(
    "-id, --instrument-decimals <number>", 
    "Decimal places for the instrument coin type. Must be <= coin::decimals(InstrumentCointType)." +
    "The sum of the instrument and quote decimals must also be <= " +
    "min(coin::decimals(InstrumentCoinType), coin::decimals(QuoteCoinType))"
  )
  .option(
    "-qc, --quote-coin-type <string>", 
    "Quote CoinType. Can be a symbol if identifying a test coin. " +
    "Otherwise, must be a fully qualified type (address::module::CoinType)."
  )
  .option(
    "-qd, --quote-decimals <number>", 
    "Decimal places for the quote coin type. Must be <= coin::decimals(QuoteCoinType). " +
    "The sum of the instrument and quote decimals must also be <= " +
    "min(coin::decimals(InstrumentCoinType), coin::decimals(QuoteCoinType))"
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
  .option(
    "-pk, --private-key [string]", 
    "Private key of the account signing this transaction. Optional. Will use the APTOS_KEY env flag if not provided.",
    process.env.APTOS_KEY,
  )
  .option(
    "-ic, --instrument-coin <string>", 
    "Instrument CoinType.",
  )
  .option(
    "-qc, --quote-coin <string>", 
    "Quote CoinType.",
  )
  .option(
    "-p, --price <number>", 
    "Limit price for the order, in terms of coin::Coin<QuoteCoinType>.",
  )
  .option(
    "-q, --quantity <number>", 
    "Quantity for the order, in terms of coin::Coin<InstrumentCoinType>",
  )
  .option(
    "-s, --side <buy | sell>", 
    "Side for the order, either buy or sell.",
  )
  .action(async (_, cmd) => {
    const { privateKey, instrumentCoin, quoteCoin, price, quantity, side } = cmd.opts();
    const txHash = await addLimitOrder(privateKey, instrumentCoin, quoteCoin, side, price, quantity)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(transactionStatusMessage(txResult), txResult)
  });

  programCommand("add-market-order")
  .option(
    "-pk, --private-key [string]", 
    "Private key of the account signing this transaction. Optional. Will use the APTOS_KEY env flag if not provided.",
    process.env.APTOS_KEY,
  )
  .option(
    "-ic, --instrument-coin <string>", 
    "Instrument CoinType.",
  )
  .option(
    "-qc, --quote-coin <string>", 
    "Quote CoinType.",
  )
  .option(
    "-q, --quantity <number>", 
    "Quantity for the order, in terms of coin::Coin<InstrumentCoinType>",
  )
  .option(
    "-s, --side <buy | sell>", 
    "Side for the order, either buy or sell.",
  )
  .option(
    "-c, --max-collateral [number]", 
    "Only required for a buy order. Max amount of coin::Coin<QuoteCoinType> allowed to be spent.",
  )
  .action(async (_, cmd) => {
    const { privateKey, instrumentCoin, quoteCoin, side, quantity, maxCollateral } = cmd.opts();
    const txHash = await addMarketOrder(privateKey, instrumentCoin, quoteCoin, side, quantity, maxCollateral)
    console.log(`Started pending transaction: ${txHash}.`)
    const txResult = await client.waitForTransactionWithResult(txHash) as Transaction_UserTransaction;
    prettyPrint(transactionStatusMessage(txResult), txResult)
  });


  programCommand("cancel-order")
  .option(
    "-pk, --private-key [string]", 
    "Private key of the account signing this transaction. Optional. Will use the APTOS_KEY env flag if not provided.",
    process.env.APTOS_KEY,
  )
  .option("-id, --order-id <number>", "Order id.")
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