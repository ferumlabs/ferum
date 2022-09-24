import { AptosAccount, TxnBuilderTypes, BCS } from "aptos";
import Config from "./config";
import { sendSignedTransactionWithAccount } from "./utils/transaction-utils";

export async function initializeUSDF(
  signerAccount: AptosAccount,
): Promise<string> {
  const entryFunctionPayload = TxnBuilderTypes.EntryFunction.natural(
    `${signerAccount.address()}::test_coins`,
    "create_usdf",
    [],
    []
  );

  return await sendSignedTransactionWithAccount(
    signerAccount,
    entryFunctionPayload
  );
}

/** Receiver needs to register the coin before they can receive it */
export async function registerUSDF(
  coinReceiver: AptosAccount,
): Promise<string> {
  const token = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(
      `${Config.getFerumAddress()}::test_coins::USDF`
    )
  );
  const entryFunctionPayload = TxnBuilderTypes.EntryFunction.natural(
    "0x1::managed_coin",
    "register",
    [token],
    []
  );
  return await sendSignedTransactionWithAccount(
    coinReceiver,
    entryFunctionPayload
  );
}

export async function mintCoin(
  signer: AptosAccount,
  amount: number,
): Promise<string> {
  const entryFunctionPayload = TxnBuilderTypes.EntryFunction.natural(
      `${Config.getFerumAddress()}::test_coins`,
      "mint_usdf",
      [],
      [
        BCS.bcsSerializeUint64(amount),
      ]
  );

  return await sendSignedTransactionWithAccount(
    signer,
    entryFunctionPayload
  );
}
