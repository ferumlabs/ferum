import {
  AptosAccount,
  TxnBuilderTypes,
  BCS,
} from "aptos";

import { sendSignedTransactionWithPrivateKey } from  "./utils/transaction-utils";
import { addAddressIfNecessary } from './utils/module-name-utils';
import { TestCoinSymbol, TEST_COINS } from  "./test-coins";

function coinTypeTags(address: string, instrumentCoin: string, quoteCoin: string) {
  if (instrumentCoin in TEST_COINS) {
    instrumentCoin = addAddressIfNecessary(address, TEST_COINS[instrumentCoin as TestCoinSymbol]);
  }
  if (quoteCoin in TEST_COINS) {
    quoteCoin = addAddressIfNecessary(address, TEST_COINS[quoteCoin as TestCoinSymbol]);
  }

  const instrumentCoinTypeTag = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(instrumentCoin)
  );

  const quoteCoinTypeTag = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(quoteCoin)
  );
  return [instrumentCoinTypeTag, quoteCoinTypeTag]
}

function toFixedPoint(n: number) {
  return Math.pow(10, 10) * n
}

export async function initializeFerum(
  signerPrivateKey: string,
) {
  const signerPrivateKeyHex = Uint8Array.from(
    Buffer.from(signerPrivateKey, "hex")
  );
  const signerAccount = new AptosAccount(signerPrivateKeyHex);
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::ferum`,
        "init_ferum",
        [],
        []
      )
    );
  return await sendSignedTransactionWithPrivateKey(signerPrivateKey, entryFunctionPayload)
}

export async function initializeMarket(
  signerPrivateKey: string,
  instrumentCoin: string,
  instrumentDecimals: number,
  quoteCoin: string,
  quoteDecimals: number,
) {
  const signerPrivateKeyHex = Uint8Array.from(
    Buffer.from(signerPrivateKey, "hex")
  );
  const signerAccount = new AptosAccount(signerPrivateKeyHex);
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::market`,
        "init_market",
        coinTypeTags(signerAccount.address().toString(), instrumentCoin, quoteCoin),
        [
          BCS.bcsSerializeU8(instrumentDecimals),
          BCS.bcsSerializeU8(quoteDecimals),
        ]
      )
    );
    return await sendSignedTransactionWithPrivateKey(signerPrivateKey, entryFunctionPayload)
}

export async function cancelOrder(
  signerPrivateKey: string,
  orderID: number,
) {
  const signerPrivateKeyHex = Uint8Array.from(
    Buffer.from(signerPrivateKey, "hex")
  );
  const signerAccount = new AptosAccount(signerPrivateKeyHex);
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::market`,
        "cancel_order",
        [],
        [
          BCS.bcsSerializeU128(orderID),
        ]
      )
    );
    return await sendSignedTransactionWithPrivateKey(signerPrivateKey, entryFunctionPayload)
}

export async function addLimitOrder(
  signerPrivateKey: string,
  instrumentCoin: string,
  quoteCoin: string,
  side: 'buy' | 'sell',
  price: number,
  quantity: number,
) {
  const signerPrivateKeyHex = Uint8Array.from(
    Buffer.from(signerPrivateKey, "hex")
  );
  const signerAccount = new AptosAccount(signerPrivateKeyHex);
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::market`,
        "add_limit_order",
        coinTypeTags(signerAccount.address().toString(), instrumentCoin, quoteCoin),
        [
          BCS.bcsSerializeU8(side === 'buy' ? 1 : 0),
          BCS.bcsSerializeUint64(toFixedPoint(price)),
          BCS.bcsSerializeUint64(toFixedPoint(quantity)),
        ]
      )
    );
    return await sendSignedTransactionWithPrivateKey(signerPrivateKey, entryFunctionPayload)
}

export async function addMarketOrder(
  signerPrivateKey: string,
  instrumentCoin: string,
  quoteCoin: string,
  side: 'buy' | 'sell',
  quantity: number,
  maxCollateral: number,
) {
  const signerPrivateKeyHex = Uint8Array.from(
    Buffer.from(signerPrivateKey, "hex")
  );
  const signerAccount = new AptosAccount(signerPrivateKeyHex);
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::market`,
        "add_market_order",
        coinTypeTags(signerAccount.address().toString(), instrumentCoin, quoteCoin),
        [
          BCS.bcsSerializeU8(side === 'buy' ? 1 : 0),
          BCS.bcsSerializeUint64(toFixedPoint(quantity)),
          BCS.bcsSerializeUint64(toFixedPoint(maxCollateral)),
        ]
      )
    );
    return await sendSignedTransactionWithPrivateKey(signerPrivateKey, entryFunctionPayload)
}
