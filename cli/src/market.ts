import {
  AptosAccount,
  AptosClient,
  TxnBuilderTypes,
  BCS,
} from "aptos";
import { TransactionPayloadEntryFunction } from "aptos/dist/transaction_builder/aptos_types";

import { client } from './aptos-client'

function coinTypeTags(instrumentCoin: string, quoteCoin: string) {
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

async function sendSignedtransaction(
  signerPrivateKey: string,
  entryFunctionPayload: TransactionPayloadEntryFunction,
) {
  const signerPrivateKeyHex = Uint8Array.from(
    Buffer.from(signerPrivateKey, "hex")
  );
  const signerAccount = new AptosAccount(signerPrivateKeyHex);
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
        `${signerAccount.address()}::market`,
        "init_ferum",
        [],
        []
      )
    );
  return await sendSignedtransaction(signerPrivateKey, entryFunctionPayload)
}

export async function initializeOrderbook(
  signerPrivateKey: string,
  instrumentCoin: string,
  quoteCoin: string
) {
  const signerPrivateKeyHex = Uint8Array.from(
    Buffer.from(signerPrivateKey, "hex")
  );
  const signerAccount = new AptosAccount(signerPrivateKeyHex);
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::market`,
        "init_book",
        coinTypeTags(instrumentCoin, quoteCoin),
        []
      )
    );
    return await sendSignedtransaction(signerPrivateKey, entryFunctionPayload)
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
    return await sendSignedtransaction(signerPrivateKey, entryFunctionPayload)
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
        coinTypeTags(instrumentCoin, quoteCoin),
        [
          BCS.bcsSerializeU8(side === 'buy' ? 1 : 0),
          BCS.bcsSerializeUint64(toFixedPoint(price)),
          BCS.bcsSerializeUint64(toFixedPoint(quantity)),
        ]
      )
    );
    return await sendSignedtransaction(signerPrivateKey, entryFunctionPayload)
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
        coinTypeTags(instrumentCoin, quoteCoin),
        [
          BCS.bcsSerializeU8(side === 'buy' ? 1 : 0),
          BCS.bcsSerializeUint64(toFixedPoint(quantity)),
          BCS.bcsSerializeUint64(toFixedPoint(maxCollateral)),
        ]
      )
    );
    return await sendSignedtransaction(signerPrivateKey, entryFunctionPayload)
}
