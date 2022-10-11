import {
  AptosAccount,
  TxnBuilderTypes,
  BCS,
} from "aptos";

import { sendSignedTransactionWithAccount } from  "./utils/transaction-utils";

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

export function addOrderTxnPayload(
  signerAccount: AptosAccount,
  instrumentCoin: string,
  quoteCoin: string,
  side: 'buy' | 'sell',
  type: 'resting' | 'ioc' | 'fok' | 'post',
  price: number,
  quantity: number,
) {
  let typ = 0;
  if (type === 'resting') {
    typ = 1;
  } else if (type === 'post') {
    typ = 2;
  } else if (type === 'ioc') {
    typ = 3;
  } else if (type === 'fok') {
    typ = 4;
  }
  return TxnBuilderTypes.EntryFunction.natural(
    `${signerAccount.address()}::market`,
    "add_limit_order_entry",
    coinTypeTags(instrumentCoin, quoteCoin),
    [
      BCS.bcsSerializeU8(side === 'buy' ? 2 : 1),
      BCS.bcsSerializeU8(typ),
      BCS.bcsSerializeUint64(price),
      BCS.bcsSerializeUint64(quantity),
      BCS.bcsSerializeStr(""),
    ]
  );
}

export async function addOrder(
  signerAccount: AptosAccount,
  instrumentCoin: string,
  quoteCoin: string,
  side: 'buy' | 'sell',
  type: 'resting' | 'ioc' | 'fok' | 'post',
  price: number,
  quantity: number,
) {
  const entryFn = addOrderTxnPayload(signerAccount, instrumentCoin, quoteCoin, side, type, price, quantity);
  return await sendSignedTransactionWithAccount(signerAccount, entryFn);
}
