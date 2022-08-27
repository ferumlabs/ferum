import fs from "fs";
import {
  AptosAccount,
  AptosClient,
  TxnBuilderTypes,
  BCS,
} from "aptos";

export const NODE_URL = "https://fullnode.devnet.aptoslabs.com";

const client = new AptosClient(NODE_URL);

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

export async function initializeOrderbook(
  signerPrivateKey: string,
  instrumentCoin: string,
  quoteCoin: string
) {
  const signerPrivateKeyHex = Uint8Array.from(
    Buffer.from(signerPrivateKey, "hex")
  );
  const signerAccount = new AptosAccount(signerPrivateKeyHex);
  const sellTokenTypeTag = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(instrumentCoin)
  );
  const buyTokenTypeTag = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(quoteCoin)
  );
  const serializer = new BCS.Serializer();
  serializer.serializeBool(false);

  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::market`,
        "init_book",
        [sellTokenTypeTag, buyTokenTypeTag],
        []
      )
    );

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

function toFixedPoint(n: number) {
  return Math.pow(10, 10) * n
}

export async function cancelOrder(
  signerPrivateKey: string,
  orderID: number,
) {
  const signerPrivateKeyHex = Uint8Array.from(
    Buffer.from(signerPrivateKey, "hex")
  );
  const signerAccount = new AptosAccount(signerPrivateKeyHex);

  const serializer = new BCS.Serializer();
  serializer.serializeBool(false);
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
  const sellTokenTypeTag = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(instrumentCoin)
  );

  const buyTokenTypeTag = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(quoteCoin)
  );
  const serializer = new BCS.Serializer();
  serializer.serializeBool(false);
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::market`,
        "add_limit_order",
        [sellTokenTypeTag, buyTokenTypeTag],
        [
          BCS.bcsSerializeU8(side === 'buy' ? 1 : 0),
          BCS.bcsSerializeUint64(toFixedPoint(price)),
          BCS.bcsSerializeUint64(toFixedPoint(quantity)),
        ]
      )
    );

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
  const sellTokenTypeTag = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(instrumentCoin)
  );

  const buyTokenTypeTag = new TxnBuilderTypes.TypeTagStruct(
    TxnBuilderTypes.StructTag.fromString(quoteCoin)
  );
  const serializer = new BCS.Serializer();
  serializer.serializeBool(false);
  const entryFunctionPayload =
    new TxnBuilderTypes.TransactionPayloadEntryFunction(
      TxnBuilderTypes.EntryFunction.natural(
        `${signerAccount.address()}::market`,
        "add_market_order",
        [sellTokenTypeTag, buyTokenTypeTag],
        [
          BCS.bcsSerializeU8(side === 'buy' ? 1 : 0),
          BCS.bcsSerializeUint64(toFixedPoint(quantity)),
          BCS.bcsSerializeUint64(toFixedPoint(maxCollateral)),
        ]
      )
    );

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
