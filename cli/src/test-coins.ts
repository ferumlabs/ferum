import {
  AptosAccount,
  TxnBuilderTypes,
  BCS,
  MaybeHexString,
  HexString,
} from "aptos";
import { client } from "./aptos-client";
import Config, { CONFIG_PATH } from "./config";
import { sendSignedTransactionWithAccount } from "./utils/transaction-utils";

export type TestCoinSymbol = 'FMA' | 'FMB';

export const TEST_COINS: {[key in TestCoinSymbol]: string} = {
  'FMA': 'test_coins::FakeMoneyA',
  'FMB': 'test_coins::FakeMoneyB',
};

// Resgiter type aliases for TEST coins.
for (let symbol in TEST_COINS) {
  Config.setAliasForType(symbol, TEST_COINS[symbol as TestCoinSymbol]);
}

/** Publish a new module to the blockchain within the specified account */
/** Currently broken until Aptos fixes their SDK */ 
export async function publishModule_broken(
  accountFrom: AptosAccount,
  moduleHex: string
): Promise<string> {
  const moduleBundlePayload =
    new TxnBuilderTypes.TransactionPayloadModuleBundle(
      new TxnBuilderTypes.ModuleBundle([
        new TxnBuilderTypes.Module(new HexString(moduleHex).toUint8Array()),
      ])
    );
  return await sendSignedTransactionWithAccount(
    accountFrom,
    moduleBundlePayload
  );
}

/** Initializes the new coin */
async function initializeCoin(
  accountFrom: AptosAccount,
  coinName: string,
  coinSymbol: string,
): Promise<string> {
  const token = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(coinName),
  );
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        "0x1::managed_coin",
        "initialize",
        [token],
        [
          BCS.bcsSerializeStr(coinName),
          BCS.bcsSerializeStr(coinSymbol),
          BCS.bcsSerializeU8(6),
          BCS.bcsSerializeBool(false),
        ]
      )
    );
  return await sendSignedTransactionWithAccount(
    accountFrom,
    entryFunctionPayload
  );
}

/** Receiver needs to register the coin before they can receive it */
async function registerCoin(
  coinReceiver: AptosAccount,
  coinName: string
): Promise<string> {
  const token = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(
      `${coinName}`
    )
  );

  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        "0x1::managed_coin",
        "register",
        [token],
        []
      )
    );

  return await sendSignedTransactionWithAccount(
    coinReceiver,
    entryFunctionPayload
  );
}

/** Mints the newly created coin to a specified receiver address */
async function mintCoin(
  coinOwner: AptosAccount,
  receiverAddress: HexString,
  amount: number,
  coinName: string
): Promise<string> {
  const token = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(
      `${coinName}`
    )
  );

  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        "0x1::managed_coin",
        "mint",
        [token],
        [
          BCS.bcsToBytes(
            TxnBuilderTypes.AccountAddress.fromHex(receiverAddress.hex())
          ),
          BCS.bcsSerializeUint64(amount),
        ]
      )
    );

    return await sendSignedTransactionWithAccount(
      coinOwner,
      entryFunctionPayload
    );
}

async function getBalance(
  accountAddress: MaybeHexString,
  coinName: string
): Promise<number> {
  try {
    const resource = await client.getAccountResource(
      accountAddress,
      `0x1::coin::CoinStore<${coinName}>`
    );
    return parseInt((resource.data as any)["coin"]["value"]);
  } catch (_) {
    return 0;
  }
}

export async function getTestCoinBalance(
  coinMasterAccount: AptosAccount,
  coinSymbol: TestCoinSymbol,
): Promise<number> {
  const coinName = Config.tryResolveAlias(coinSymbol);
  return await getBalance(
    coinMasterAccount.address(),
    coinName
  );
}

// Sets up the test coin for the specified account.
export async function createTestCoin(
  coinMasterAccount: AptosAccount,
  coinSymbol: TestCoinSymbol,
) {
  const coinName = Config.tryResolveAlias(coinSymbol);

  // 0. Show current balance.
  const currentBalance = await getBalance(
    coinMasterAccount.address(), 
    coinName,
  );
  console.log(
    `\nCurrent ${coinSymbol} balance for ${coinMasterAccount.address()}: ${currentBalance}\n`
  );

  let txHash;

  // 1. Initialize coin.
  console.log(`Initializing ${coinSymbol} | ${coinName}...`);
  txHash = await initializeCoin(
    coinMasterAccount,
    coinName,
    coinSymbol,
  );
  console.log(`Transaction Hash: ${txHash}`);
  await client.waitForTransaction(txHash);

  console.log(`\n---\n`);

  // 2. Register coin to receive it.
  console.log(`Registering ${coinSymbol}`);
  txHash = await registerCoin(
    coinMasterAccount,
    coinName
  );
  console.log(`Transaction Hash: ${txHash}`);
  await client.waitForTransaction(txHash);

  console.log(`\n---\n`);

  // 3. Mint new coins.
  console.log(`Minting 100 ${coinSymbol}`);
  txHash = await mintCoin(
    coinMasterAccount,
    coinMasterAccount.address(),
    100 * Math.pow(10, 6),
    coinName
  );
  console.log(`Transaction Hash: ${txHash}`);
  await client.waitForTransaction(txHash);

  console.log(`\n---\n`);

  // 4. Check new balance
  console.log(
    `New ${coinSymbol} balance: ${await getBalance(
      coinMasterAccount.address(),
      coinName
    )}`
  );

  console.log(`\n---\n`);
}
