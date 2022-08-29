import { AptosClient } from "aptos";

export const NODE_URL = "https://fullnode.devnet.aptoslabs.com/v1";

export const client = new AptosClient(NODE_URL);
