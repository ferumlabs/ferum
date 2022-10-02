import { AptosAccount, TxnBuilderTypes } from "aptos";
import { getClient } from '../aptos-client';


export async function sendSignedTransactionWithPrivateKey(
  signerPrivateKey: string,
  entryFunction: TxnBuilderTypes.EntryFunction,
) {
  const signerPrivateKeyHex = Uint8Array.from(
    Buffer.from(signerPrivateKey, "hex")
  );
  const signerAccount = new AptosAccount(signerPrivateKeyHex);
  return await sendSignedTransactionWithAccount(signerAccount, entryFunction)
}

export async function sendSignedTransactionWithAccount(
  signerAccount: AptosAccount,
  entryFunction: TxnBuilderTypes.EntryFunction,
) {

  const entryFunctionPayload = new TxnBuilderTypes.TransactionPayloadEntryFunction(entryFunction);

  // Ge the latest sequence number and chain id.
  const [{ sequence_number: sequenceNumber }, chainId] = await Promise.all([
    getClient().getAccount(signerAccount.address()),
    getClient().getChainId(),
  ]);

  const rawTxn = new TxnBuilderTypes.RawTransaction(
      // Transaction sender account address
      TxnBuilderTypes.AccountAddress.fromHex(signerAccount.address()),
      BigInt(sequenceNumber),
      entryFunctionPayload,
      // Max gas unit to spend
      BigInt(2000),
      // Gas price per unit
      BigInt(100),
      // Expiration timestamp. Transaction is discarded if it is not executed within 10 seconds from now.
      BigInt(Math.floor(Date.now() / 1000) + 10),
      new TxnBuilderTypes.ChainId(chainId),
  );
  const signedTxn = await getClient().signTransaction(signerAccount, rawTxn);
  const pendingTxn = await getClient().submitSignedBCSTransaction(signedTxn);
  return pendingTxn.hash;
}

export async function simulateTransactionWithAccount(
  signerAccount: AptosAccount,
  entryFunction: TxnBuilderTypes.EntryFunction,
) {

  const entryFunctionPayload = new TxnBuilderTypes.TransactionPayloadEntryFunction(entryFunction);

  // Ge the latest sequence number and chain id.
  const [{ sequence_number: sequenceNumber }, chainId] = await Promise.all([
    getClient().getAccount(signerAccount.address()),
    getClient().getChainId(),
  ]);

  const rawTxn = new TxnBuilderTypes.RawTransaction(
      // Transaction sender account address
      TxnBuilderTypes.AccountAddress.fromHex(signerAccount.address()),
      BigInt(sequenceNumber),
      entryFunctionPayload,
      // Max gas unit to spend
      BigInt(2000),
      // Gas price per unit
      BigInt(100),
      // Expiration timestamp. Transaction is discarded if it is not executed within 10 seconds from now.
      BigInt(Math.floor(Date.now() / 1000) + 10),
      new TxnBuilderTypes.ChainId(chainId),
  );
  const simulatedTxn = await getClient().simulateTransaction(signerAccount, rawTxn);
  return simulatedTxn[0];
}