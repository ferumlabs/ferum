
import fs from "fs";
import { AptosAccount, AptosClient, TxnBuilderTypes, BCS, MaybeHexString, HexString } from "aptos";
import { client } from './aptos-client'

/** Publish a new module to the blockchain within the specified account */
export async function publishModule(accountFrom: AptosAccount, moduleHex: string): Promise<string> {
  const moduleBundlePayload = new TxnBuilderTypes.TransactionPayloadModuleBundle(
    new TxnBuilderTypes.ModuleBundle([new TxnBuilderTypes.Module(new HexString(moduleHex).toUint8Array())]),
  );

  const [{ sequence_number: sequenceNumber }, chainId] = await Promise.all([
    client.getAccount(accountFrom.address()),
    client.getChainId(),
  ]);

  const rawTxn = new TxnBuilderTypes.RawTransaction(
    TxnBuilderTypes.AccountAddress.fromHex(accountFrom.address()),
    BigInt(sequenceNumber),
    moduleBundlePayload,
    1000n,
    1n,
    BigInt(Math.floor(Date.now() / 1000) + 10),
    new TxnBuilderTypes.ChainId(chainId),
  );

  const bcsTxn = AptosClient.generateBCSTransaction(accountFrom, rawTxn);
  const transactionRes = await client.submitSignedBCSTransaction(bcsTxn);

  return transactionRes.hash;
}
/** Initializes the new coin */
async function initializeCoin(accountFrom: AptosAccount, coinTypeAddress: HexString, coinName: string): Promise<string> {
  const token = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(`${coinTypeAddress.hex()}::${coinName}`),
  );

  const serializer = new BCS.Serializer();
  serializer.serializeBool(false);

  const entryFunctionPayload = new TxnBuilderTypes.TransactionPayloadEntryFunction(
    TxnBuilderTypes.EntryFunction.natural(
      "0x1::managed_coin",
      "initialize",
      [token],
      [
        BCS.bcsSerializeStr(coinName),
        BCS.bcsSerializeStr(coinName),
        BCS.bcsSerializeU8(6),
        serializer.getBytes(),
      ],
    ),
  );

  const [{ sequence_number: sequenceNumber }, chainId] = await Promise.all([
    client.getAccount(accountFrom.address()),
    client.getChainId(),
  ]);

  const rawTxn = new TxnBuilderTypes.RawTransaction(
    TxnBuilderTypes.AccountAddress.fromHex(accountFrom.address()),
    BigInt(sequenceNumber),
    entryFunctionPayload,
    1000n,
    1n,
    BigInt(Math.floor(Date.now() / 1000) + 10),
    new TxnBuilderTypes.ChainId(chainId),
  );

  const bcsTxn = AptosClient.generateBCSTransaction(accountFrom, rawTxn);
  const pendingTxn = await client.submitSignedBCSTransaction(bcsTxn);

  return pendingTxn.hash;
}

/** Receiver needs to register the coin before they can receive it */
async function registerCoin(coinReceiver: AptosAccount, coinTypeAddress: HexString, coinName: string): Promise<string> {
  const token = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(`${coinTypeAddress.hex()}::${coinName}`),
  );

  const entryFunctionPayload = new TxnBuilderTypes.TransactionPayloadEntryFunction(
    TxnBuilderTypes.EntryFunction.natural("0x1::coins", "register", [token], []),
  );

  const [{ sequence_number: sequenceNumber }, chainId] = await Promise.all([
    client.getAccount(coinReceiver.address()),
    client.getChainId(),
  ]);

  const rawTxn = new TxnBuilderTypes.RawTransaction(
    TxnBuilderTypes.AccountAddress.fromHex(coinReceiver.address()),
    BigInt(sequenceNumber),
    entryFunctionPayload,
    1000n,
    1n,
    BigInt(Math.floor(Date.now() / 1000) + 10),
    new TxnBuilderTypes.ChainId(chainId),
  );

  const bcsTxn = AptosClient.generateBCSTransaction(coinReceiver, rawTxn);
  const pendingTxn = await client.submitSignedBCSTransaction(bcsTxn);

  return pendingTxn.hash;
}

/** Mints the newly created coin to a specified receiver address */
async function mintCoin(
  coinOwner: AptosAccount,
  coinTypeAddress: HexString,
  receiverAddress: HexString,
  amount: number,
  coinName: string,
): Promise<string> {
  const token = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(`${coinTypeAddress.hex()}::${coinName}`),
  );

  const entryFunctionPayload = new TxnBuilderTypes.TransactionPayloadEntryFunction(
    TxnBuilderTypes.EntryFunction.natural(
      "0x1::managed_coin",
      "mint",
      [token],
      [BCS.bcsToBytes(TxnBuilderTypes.AccountAddress.fromHex(receiverAddress.hex())), BCS.bcsSerializeUint64(amount)],
    ),
  );

  const [{ sequence_number: sequenceNumber }, chainId] = await Promise.all([
    client.getAccount(coinOwner.address()),
    client.getChainId(),
  ]);

  const rawTxn = new TxnBuilderTypes.RawTransaction(
    TxnBuilderTypes.AccountAddress.fromHex(coinOwner.address()),
    BigInt(sequenceNumber),
    entryFunctionPayload,
    1000n,
    1n,
    BigInt(Math.floor(Date.now() / 1000) + 10),
    new TxnBuilderTypes.ChainId(chainId),
  );

  const bcsTxn = AptosClient.generateBCSTransaction(coinOwner, rawTxn);
  const pendingTxn = await client.submitSignedBCSTransaction(bcsTxn);
  return pendingTxn.hash;
}

async function getBalance(accountAddress: MaybeHexString, coinTypeAddress: HexString, coinName: string): Promise<number> {
  try {
    const resource = await client.getAccountResource(
      accountAddress,
      `0x1::coin::CoinStore<${coinTypeAddress.hex()}::${coinName}>`,
    );
    return parseInt((resource.data as any)["coin"]["value"]);
  } catch (_) {
    return 0;
  }
}

export async function getTestCoinBalance(privateKey: string, coinName: string) : Promise<number> {
  const privateKeyHex = Uint8Array.from(
    Buffer.from(privateKey, "hex"),
  );
  const coinMasterAccount = new AptosAccount(privateKeyHex);
  return await getBalance(coinMasterAccount.address(), coinMasterAccount.address(), coinName)
}

export async function createTestCoin(privateKey: string, coinModuleByteCodePath: string, coinName: string) {
  const privateKeyHex = Uint8Array.from(
    Buffer.from(privateKey, "hex"),
  );
  const coinMasterAccount = new AptosAccount(privateKeyHex);

  console.log("\n=== Name, Account, Coins ===");
  console.log(`Current Balance: ${coinMasterAccount.address()}: ${await getBalance(coinMasterAccount.address(), coinMasterAccount.address(), coinName)}`);

  const moduleHex = fs.readFileSync(coinModuleByteCodePath).toString("hex");
  
  let txHash;

  // 1. Publish coin module.
  console.log("Publishing coin module...");
  txHash = await publishModule(coinMasterAccount, moduleHex);
  console.log(txHash)
  console.log(await client.waitForTransaction(txHash));

  // 2. Initialize coin.
  console.log("Initializing new coin...");
  txHash = await initializeCoin(coinMasterAccount, coinMasterAccount.address(), coinName);
  console.log(txHash)
  console.log(await client.waitForTransaction(txHash));

  // 3. Register coin to receive it.
  console.log("Register coin");
  txHash = await registerCoin(coinMasterAccount, coinMasterAccount.address(), coinName);
  console.log(txHash)
  await client.waitForTransaction(txHash);

  // 4. Mint new coins.
  console.log("Minting 100 coins");
  txHash = await mintCoin(coinMasterAccount, coinMasterAccount.address(), coinMasterAccount.address(), 100 * Math.pow(10, 6), coinName);
  console.log(txHash)
  await client.waitForTransaction(txHash);

  // 5. Check new balance
  console.log(`New balance: ${await getBalance(coinMasterAccount.address(), coinMasterAccount.address(), coinName)}`);
}
