import {
  AptosAccount,
  TxnBuilderTypes,
  BCS,
  AptosClient,
} from "aptos";

import { TransactionPayload } from "aptos/dist/transaction_builder/aptos_types";

const FERUM = "0xc1a5407f567d330840f79b8612ec023409d382af407ee21e7de130bf0e4ce437"
const NODE_URL = "https://fullnode.devnet.aptoslabs.com/v1";
const client = new AptosClient(NODE_URL);

// 0. Helper function to send signed transactions.
async function sendSignedTransactionWithAccount(
  signerAccount: AptosAccount,
  entryFunctionPayload: TransactionPayload,
) {
  const rawTxn = await client.generateRawTransaction(signerAccount.address(), entryFunctionPayload);
  const bcsTxn = AptosClient.generateBCSTransaction(signerAccount, rawTxn);
  const pendingTxn = await client.submitSignedBCSTransaction(bcsTxn);
  return pendingTxn.hash;
}

// 1. Must register the coin before you receive it. 
async function registerCoin(
  signer: AptosAccount,
): Promise<string> {
  const token = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(
      `${FERUM}::test_coins::USDF`
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
    signer,
    entryFunctionPayload
  );
}

// 2. Function to mint test USDF coins into account. 
async function mintCoin(
  signer: AptosAccount,
  amount: number,
): Promise<string> {
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${FERUM}::test_coins`,
        "mint_fusd",
        [],
        [
          BCS.bcsSerializeUint64(amount),
        ]
      )
    );
    return await sendSignedTransactionWithAccount(
      signer,
      entryFunctionPayload
    );
}

async function runFaucet() {
  let privateKey = "7b26f9d6e14f993097db78506038e5cf730927adc2166682bd980f82228d297b";
  let signer = new AptosAccount(Uint8Array.from(Buffer.from(privateKey, "hex")));

  // Register for USDF.
  let txHash = await registerCoin(signer);
  console.log(`Register for coin transaction hash: ${txHash}`);
  await client.waitForTransaction(txHash);

  // // Mint some sweet USDF.
  // txHash = await mintCoin(signer, 100000);
  // console.log(`Minting coin transaction hash: ${txHash}`);
}

runFaucet();