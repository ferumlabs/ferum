import fs from "fs";
import {
  AptosAccount,
  AptosClient,
  TxnBuilderTypes,
  BCS,
  MaybeHexString,
  HexString,
} from "aptos";
import { client } from "./aptos-client";
import { addCoinAddressIfNecessary } from "./utils/module-name-utils";
import { sendSignedTransactionWithAccount } from "./utils/transaction-utils";

/** Publish a new module to the blockchain within the specified account */
export async function publishModule(
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
  coinTypeAddress: HexString,
  coinName: string
): Promise<string> {
  const token = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(
      `${coinTypeAddress.hex()}::${coinName}`
    )
  );

  const serializer = new BCS.Serializer();
  serializer.serializeBool(false);

  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        "0x1::managed_coin",
        "initialize",
        [token],
        [
          BCS.bcsSerializeStr(coinName),
          BCS.bcsSerializeStr(coinName),
          BCS.bcsSerializeU8(6),
          serializer.getBytes(),
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
  coinTypeAddress: HexString,
  coinName: string
): Promise<string> {
  const token = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(
      `${coinTypeAddress.hex()}::${coinName}`
    )
  );

  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        "0x1::coins",
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
  coinTypeAddress: HexString,
  receiverAddress: HexString,
  amount: number,
  coinName: string
): Promise<string> {
  const token = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(
      `${coinTypeAddress.hex()}::${coinName}`
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
  coinTypeAddress: HexString,
  coinName: string
): Promise<number> {
  try {
    const resource = await client.getAccountResource(
      accountAddress,
      `0x1::coin::CoinStore<${coinTypeAddress.hex()}::${coinName}>`
    );
    return parseInt((resource.data as any)["coin"]["value"]);
  } catch (_) {
    return 0;
  }
}

export async function getTestCoinBalance(
  privateKey: string,
  coinName: string
): Promise<number> {
  const privateKeyHex = Uint8Array.from(Buffer.from(privateKey, "hex"));
  const coinMasterAccount = new AptosAccount(privateKeyHex);
  return await getBalance(
    coinMasterAccount.address(),
    coinMasterAccount.address(),
    coinName
  );
}

export async function createTestCoin(
  privateKey: string,
  coinModuleByteCodePath: string,
  coinName: string
) {
  const privateKeyHex = Uint8Array.from(Buffer.from(privateKey, "hex"));
  const coinMasterAccount = new AptosAccount(privateKeyHex);
  coinName = addCoinAddressIfNecessary(
    coinMasterAccount.address().toString(),
    coinName
  );
  console.log("\n=== Name, Account, Coins ===");
  console.log(
    `Current Balance: ${coinMasterAccount.address()}: ${await getBalance(
      coinMasterAccount.address(),
      coinMasterAccount.address(),
      coinName
    )}`
  );

  const moduleHex = fs.readFileSync(coinModuleByteCodePath).toString("hex");

  let txHash;

  // 1. Publish coin module.
  console.log("Publishing coin module...");
  txHash = await publishModule(coinMasterAccount, moduleHex);
  console.log(txHash);
  console.log(await client.waitForTransaction(txHash));

  // 2. Initialize coin.
  console.log("Initializing new coin...");
  txHash = await initializeCoin(
    coinMasterAccount,
    coinMasterAccount.address(),
    coinName
  );
  console.log(txHash);
  console.log(await client.waitForTransaction(txHash));

  // 3. Register coin to receive it.
  console.log("Register coin");
  txHash = await registerCoin(
    coinMasterAccount,
    coinMasterAccount.address(),
    coinName
  );
  console.log(txHash);
  await client.waitForTransaction(txHash);

  // 4. Mint new coins.
  console.log("Minting 100 coins");
  txHash = await mintCoin(
    coinMasterAccount,
    coinMasterAccount.address(),
    coinMasterAccount.address(),
    100 * Math.pow(10, 6),
    coinName
  );
  console.log(txHash);
  await client.waitForTransaction(txHash);

  // 5. Check new balance
  console.log(
    `New balance: ${await getBalance(
      coinMasterAccount.address(),
      coinMasterAccount.address(),
      coinName
    )}`
  );
}
