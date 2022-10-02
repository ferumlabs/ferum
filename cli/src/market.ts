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
  const entryFunction = TxnBuilderTypes.EntryFunction.natural(
    `${signerAccount.address()}::admin`,
    "init_ferum",
    [],
    []
  );
  return await sendSignedTransactionWithAccount(signerAccount, entryFunction)
}

export async function initializeMarket(
  signerAccount: AptosAccount,
  instrumentCoin: string,
  instrumentDecimals: number,
  quoteCoin: string,
  quoteDecimals: number,
) {
  const entryFunction = TxnBuilderTypes.EntryFunction.natural(
    `${signerAccount.address()}::market`,
    "init_market_entry",
    coinTypeTags(instrumentCoin, quoteCoin),
    [
      BCS.bcsSerializeU8(instrumentDecimals),
      BCS.bcsSerializeU8(quoteDecimals),
    ]
  );
  return await sendSignedTransactionWithAccount(signerAccount, entryFunction)
}

export async function cancelOrder(
  signerAccount: AptosAccount,
  orderID: number,
  instrumentCoinType: string,
  quoteCoinType: string,
) {
  const entryFunction = TxnBuilderTypes.EntryFunction.natural(
    `${signerAccount.address()}::market`,
    "cancel_order_entry",
    coinTypeTags(instrumentCoinType, quoteCoinType),
    [
      BCS.bcsSerializeU128(orderID),
    ]
  );
  return await sendSignedTransactionWithAccount(signerAccount, entryFunction)
}

export function addLimitTxnPayload(
  signerAccount: AptosAccount,
  instrumentCoin: string,
  quoteCoin: string,
  side: 'buy' | 'sell',
  price: number,
  quantity: number,
) {
  return TxnBuilderTypes.EntryFunction.natural(
    `${signerAccount.address()}::market`,
    "add_limit_order_entry",
    coinTypeTags(instrumentCoin, quoteCoin),
    [
      BCS.bcsSerializeU8(side === 'buy' ? 2 : 1),
      BCS.bcsSerializeUint64(price),
      BCS.bcsSerializeUint64(quantity),
      BCS.bcsSerializeStr(""),
    ]
  );
}

export async function addLimitOrder(
  signerAccount: AptosAccount,
  instrumentCoin: string,
  quoteCoin: string,
  side: 'buy' | 'sell',
  price: number,
  quantity: number,
) {
  const entryFn = addLimitTxnPayload(signerAccount, instrumentCoin, quoteCoin, side, price, quantity);
  return await sendSignedTransactionWithAccount(signerAccount, entryFn);
}

export async function addMarketOrder(
  signerAccount: AptosAccount,
  instrumentCoin: string,
  quoteCoin: string,
  side: 'buy' | 'sell',
  quantity: number,
  maxCollateral: number,
) {
  const entryFunction = TxnBuilderTypes.EntryFunction.natural(
    `${signerAccount.address()}::market`,
    "add_market_order_entry",
    coinTypeTags(instrumentCoin, quoteCoin),
    [
      BCS.bcsSerializeU8(side === 'buy' ? 2 : 1),
      BCS.bcsSerializeUint64(quantity),
      BCS.bcsSerializeUint64(maxCollateral),
      BCS.bcsSerializeStr(""),
    ]
  );
  return await sendSignedTransactionWithAccount(signerAccount, entryFunction)
}
