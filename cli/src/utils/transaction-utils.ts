import {
  AptosAccount,
  AptosClient,
  TxnBuilderTypes,
  BCS,
} from "aptos";
import { TransactionPayload } from "aptos/dist/transaction_builder/aptos_types";

import { client } from '../aptos-client'

export async function sendSignedTransactionWithPrivateKey(
  signerPrivateKey: string,
  entryFunctionPayload: TransactionPayload,
) {
  const signerPrivateKeyHex = Uint8Array.from(
    Buffer.from(signerPrivateKey, "hex")
  );
  const signerAccount = new AptosAccount(signerPrivateKeyHex);
  return await sendSignedTransactionWithAccount(signerAccount, entryFunctionPayload)
}

export async function sendSignedTransactionWithAccount(
  signerAccount: AptosAccount,
  entryFunctionPayload: TransactionPayload,
) {
  const rawTxn = await client.generateRawTransaction(signerAccount.address(), entryFunctionPayload);
  const bcsTxn = AptosClient.generateBCSTransaction(signerAccount, rawTxn);
  const pendingTxn = await client.submitSignedBCSTransaction(bcsTxn);
  return pendingTxn.hash;
}