import { AptosClient, FaucetClient } from "aptos";
import { getEnv } from "./utils/env";

const DEVNET_NODE_URL = "https://fullnode.devnet.aptoslabs.com/v1";
const DEVNET_FAUCET_URL = "https://faucet.devnet.aptoslabs.com";
const TESTNET_NODE_URL = "https://fullnode.testnet.aptoslabs.com/v1";
const TESTNET_FAUCET_URL = "https://faucet.testnet.aptoslabs.com";

const DevnetClient = new AptosClient(DEVNET_NODE_URL);
const DevnetFaucetClient = new FaucetClient(DEVNET_NODE_URL, DEVNET_FAUCET_URL);

const TestnetClient = new AptosClient(TESTNET_NODE_URL);
const TestnetFaucetClient = new FaucetClient(TESTNET_NODE_URL, TESTNET_FAUCET_URL);

export function getClient(): AptosClient {
  const env = getEnv();
  switch(env) {
    case 'devnet':
      return DevnetClient;
    case 'testnet':
      return TestnetClient;  
  }
  assertUnreachable(env);
}

export function getFaucetClient(): FaucetClient {
  const env = getEnv();
  switch(env) {
    case 'devnet':
      return DevnetFaucetClient;
    case 'testnet':
      return TestnetFaucetClient;  
  }
  assertUnreachable(env);
}

export function getNodeURL(): string {
  const env = getEnv();
  switch(env) {
    case 'devnet':
      return DEVNET_NODE_URL;
    case 'testnet':
      return TESTNET_NODE_URL;  
  }
  assertUnreachable(env);
}

function assertUnreachable(x: never): never {
  throw new Error("Didn't expect to get here");
}
