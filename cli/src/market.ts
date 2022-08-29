import {
  AptosAccount,
  TxnBuilderTypes,
  BCS,
} from "aptos";

import { sendSignedTransactionWithAccount, sendSignedTransactionWithPrivateKey } from  "./utils/transaction-utils";

function coinTypeTags(instrumentCoin: string, quoteCoin: string) {
  const instrumentCoinTypeTag = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(instrumentCoin)
  );

  const quoteCoinTypeTag = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(quoteCoin)
  );
  return [instrumentCoinTypeTag, quoteCoinTypeTag]
}

export async function initializeFerum(
  signerAccount: AptosAccount,
) {
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::ferum`,
        "init_ferum",
        [],
        []
      )
    );
  return await sendSignedTransactionWithAccount(signerAccount, entryFunctionPayload)
}

export async function initializeMarket(
  signerAccount: AptosAccount,
  instrumentCoin: string,
  instrumentDecimals: number,
  quoteCoin: string,
  quoteDecimals: number,
) {
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::market`,
        "init_market",
        coinTypeTags(instrumentCoin, quoteCoin),
        [
          BCS.bcsSerializeU8(instrumentDecimals),
          BCS.bcsSerializeU8(quoteDecimals),
        ]
      )
    );
    return await sendSignedTransactionWithAccount(signerAccount, entryFunctionPayload)
}

export async function cancelOrder(
  signerAccount: AptosAccount,
  orderID: number,
  instrumentCoinType: string,
  quoteCoinType: string,
) {
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::market`,
        "cancel_order",
        coinTypeTags(instrumentCoinType, quoteCoinType),
        [
          BCS.bcsSerializeU128(orderID),
        ]
      )
    );
    return await sendSignedTransactionWithAccount(signerAccount, entryFunctionPayload)
}

export async function addLimitOrder(
  signerAccount: AptosAccount,
  instrumentCoin: string,
  quoteCoin: string,
  side: 'buy' | 'sell',
  price: number,
  quantity: number,
) {
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::market`,
        "add_limit_order",
        coinTypeTags(instrumentCoin, quoteCoin),
        [
          BCS.bcsSerializeU8(side === 'buy' ? 1 : 0),
          BCS.bcsSerializeUint64(price),
          BCS.bcsSerializeUint64(quantity),
        ]
      )
    );
    return await sendSignedTransactionWithAccount(signerAccount, entryFunctionPayload)
}

export async function addMarketOrder(
  signerAccount: AptosAccount,
  instrumentCoin: string,
  quoteCoin: string,
  side: 'buy' | 'sell',
  quantity: number,
  maxCollateral: number,
) {
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::market`,
        "add_market_order",
        coinTypeTags(instrumentCoin, quoteCoin),
        [
          BCS.bcsSerializeU8(side === 'buy' ? 1 : 0),
          BCS.bcsSerializeUint64(quantity),
          BCS.bcsSerializeUint64(maxCollateral),
        ]
      )
    );
    return await sendSignedTransactionWithAccount(signerAccount, entryFunctionPayload)
}
