import { AptosClient, FaucetClient } from "aptos";
import Config from "./config";
import { assertUnreachable } from "./utils/types";

const DEVNET_NODE_URL = "https://fullnode.devnet.aptoslabs.com/v1";
const DEVNET_FAUCET_URL = "https://faucet.devnet.aptoslabs.com";
const TESTNET_NODE_URL = "https://fullnode.testnet.aptoslabs.com/v1";
const TESTNET_FAUCET_URL = "https://faucet.testnet.aptoslabs.com";
const LOCAL_NODE_URL = "http://localhost:8080/v1";
const LOCAL_FAUCET_URL = "http://localhost:8081";

const DevnetClient = new AptosClient(DEVNET_NODE_URL);
const DevnetFaucetClient = new FaucetClient(DEVNET_NODE_URL, DEVNET_FAUCET_URL);

const TestnetClient = new AptosClient(TESTNET_NODE_URL);
const TestnetFaucetClient = new FaucetClient(TESTNET_NODE_URL, TESTNET_FAUCET_URL);

const LocalClient = new AptosClient(LOCAL_NODE_URL);
const LocalFaucetClient = new FaucetClient(LOCAL_NODE_URL, LOCAL_FAUCET_URL);

export function getClient(): AptosClient {
  const env = Config.getEnv();
  switch(env) {
    case 'devnet':
      return DevnetClient;
    case 'testnet':
      return TestnetClient;
    case 'local':
      return LocalClient;
  }
  assertUnreachable(env);
}

export function getFaucetClient(): FaucetClient {
  const env = Config.getEnv();
  switch(env) {
    case 'devnet':
      return DevnetFaucetClient;
    case 'testnet':
      return TestnetFaucetClient;
    case 'local':
      return LocalFaucetClient;
  }
  assertUnreachable(env);
}

export function getNodeURL(): string {
  const env = Config.getEnv();
  switch(env) {
    case 'devnet':
      return DEVNET_NODE_URL;
    case 'testnet':
      return TESTNET_NODE_URL;
    case 'local':
      return LOCAL_NODE_URL;
  }
  assertUnreachable(env);
}

