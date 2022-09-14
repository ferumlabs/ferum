import {
  AptosAccount,
  TxnBuilderTypes,
  BCS,
  MaybeHexString,
  HexString,
} from "aptos";
import { client } from "./aptos-client";
import config from "./config";
import { sendSignedTransactionWithAccount } from "./utils/transaction-utils";

async function initializeCoin(
  signerAccount: AptosAccount,
): Promise<string> {
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::test_coins`,
        "create_usdf",
        [],
        []
      )
    );
  return await sendSignedTransactionWithAccount(
    signerAccount,
    entryFunctionPayload
  );
}

/** Receiver needs to register the coin before they can receive it */
async function registerCoin(
  coinReceiver: AptosAccount,
): Promise<string> {
  const token = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(
      `${config.getFerumAddress()}::test_coins::USDF`
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

async function mintCoin(
  signer: AptosAccount,
  amount: number,
): Promise<string> {
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${config.getFerumAddress()}::test_coins`,
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

// Sets up the test coin for the specified account.
export async function createUSDF(
  signer: AptosAccount,
) {
  //let txHash = await initializeCoin(coinMasterAccount);
  //console.log(`Initalize coin transaction hash: ${txHash}`);
  //await client.waitForTransaction(txHash);
  
  // 2. Register receiver for coin.
  // let txHash = await registerCoin(signer);
  // console.log(`Register for coin transaction hash: ${txHash}`);
  // await client.waitForTransaction(txHash);


  // 3. Mint USDF coin.
  let txHash = await mintCoin(signer, 100000);
  console.log(`Minting coin transaction hash: ${txHash}`);
}
