import { AptosClient } from "aptos";

const NODE_URL = "https://fullnode.devnet.aptoslabs.com";

export const client = new AptosClient(NODE_URL);
