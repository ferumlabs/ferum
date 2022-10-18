import { AptosAccount, TxnBuilderTypes } from "aptos";
import { getClient } from '../aptos-client';

export type TxOptions = {
  maxGas?: number,
  seqNum?: string | number | null,
};

const DefaultTxOpts: TxOptions = {};

export async function sendSignedTransactionWithPrivateKey(
  signerPrivateKey: string,
  entryFunction: TxnBuilderTypes.EntryFunction,
  opts: TxOptions = DefaultTxOpts,
) {
  const signerPrivateKeyHex = Uint8Array.from(
    Buffer.from(signerPrivateKey, "hex")
  );
  const signerAccount = new AptosAccount(signerPrivateKeyHex);
  return await sendSignedTransactionWithAccount(signerAccount, entryFunction, opts)
}

export async function sendSignedTransactionWithAccount(
  signerAccount: AptosAccount,
  entryFunction: TxnBuilderTypes.EntryFunction,
  opts: TxOptions = DefaultTxOpts,
) {
  const entryFunctionPayload = new TxnBuilderTypes.TransactionPayloadEntryFunction(entryFunction);

  let {maxGas = 2000, seqNum = null} = opts;

  // Ge the latest sequence number and chain id.
  let chain: number = 0;
  if (!seqNum) {
    const [{ sequence_number: sequenceNumber }, chainId] = await Promise.all([
      getClient().getAccount(signerAccount.address()),
      getClient().getChainId(),
    ]);
    seqNum = sequenceNumber;
    chain = chainId;
  } else {
    chain = await getClient().getChainId();
  }

  const rawTxn = new TxnBuilderTypes.RawTransaction(
      // Transaction sender account address
      TxnBuilderTypes.AccountAddress.fromHex(signerAccount.address()),
      BigInt(seqNum),
      entryFunctionPayload,
      // Max gas unit to spend
      BigInt(maxGas),
      // Gas price per unit
      BigInt(100),
      // Expiration timestamp. Transaction is discarded if it is not executed within 10 seconds from now.
      BigInt(Math.floor(Date.now() / 1000) + 10),
      new TxnBuilderTypes.ChainId(chain),
  );
  const signedTxn = await getClient().signTransaction(signerAccount, rawTxn);
  const pendingTxn = await getClient().submitSignedBCSTransaction(signedTxn);
  return pendingTxn.hash;
}

export async function simulateTransactionWithAccount(
  signerAccount: AptosAccount,
  entryFunction: TxnBuilderTypes.EntryFunction,
  {maxGas, seqNum}: TxOptions = DefaultTxOpts,
) {
  const entryFunctionPayload = new TxnBuilderTypes.TransactionPayloadEntryFunction(entryFunction);

  // Ge the latest sequence number and chain id.
  let chain: number = 0;
  if (!seqNum) {
    const [{ sequence_number: sequenceNumber }, chainId] = await Promise.all([
      getClient().getAccount(signerAccount.address()),
      getClient().getChainId(),
    ]);
    seqNum = sequenceNumber;
    chain = chainId;
  } else {
    chain = await getClient().getChainId();
  }

  const rawTxn = new TxnBuilderTypes.RawTransaction(
      // Transaction sender account address
      TxnBuilderTypes.AccountAddress.fromHex(signerAccount.address()),
      BigInt(seqNum),
      entryFunctionPayload,
      // Max gas unit to spend
      BigInt(maxGas),
      // Gas price per unit
      BigInt(100),
      // Expiration timestamp. Transaction is discarded if it is not executed within 10 seconds from now.
      BigInt(Math.floor(Date.now() / 1000) + 10),
      new TxnBuilderTypes.ChainId(chain),
  );
  const simulatedTxn = await getClient().simulateTransaction(signerAccount, rawTxn);
  console.log(simulatedTxn);
  return simulatedTxn[0].hash;
}