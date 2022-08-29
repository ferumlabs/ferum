import { AptosClient, FaucetClient } from "aptos";

export const NODE_URL = "https://fullnode.devnet.aptoslabs.com/v1";
const FAUCET_URL = "https://faucet.devnet.aptoslabs.com";

export const client = new AptosClient(NODE_URL);

export const faucetClient = new FaucetClient(NODE_URL, FAUCET_URL);
