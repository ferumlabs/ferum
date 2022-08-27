import {
  AptosAccount,
  AptosClient,
  TxnBuilderTypes,
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
  const [{ sequence_number: sequenceNumber }, chainId] = await Promise.all([
    client.getAccount(signerAccount.address()),
    client.getChainId(),
  ]);

  const rawTxn = new TxnBuilderTypes.RawTransaction(
    TxnBuilderTypes.AccountAddress.fromHex(signerAccount.address()),
    BigInt(sequenceNumber),
    entryFunctionPayload,
    1000n,
    1n,
    BigInt(Math.floor(Date.now() / 1000) + 10),
    new TxnBuilderTypes.ChainId(chainId)
  );

  const bcsTxn = AptosClient.generateBCSTransaction(signerAccount, rawTxn);
  const pendingTxn = await client.submitSignedBCSTransaction(bcsTxn);

  return pendingTxn.hash;
}