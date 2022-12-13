# Ferum Contracts


## Commands

Easiest way to interact with Ferum is through the Aptos CLI, and the easiest way to get the CLI is by downloading the latest & greatest 

### Publish to New Account

If you renamed or removed a module, most likely your contract will not be backwards compatible and you will need to publish to a new account.

A couple of quick steps you can follow to publish to a new acccount. 

1. Use Petra wallet to create a new account. 
2. Use an account with some test APT to fund it (1 APT is usually enough) — you can try to use the faucet, but 9/10, it's broken. 
4. Update the contract address in `Move.toml` to match the new account address. 
5. Export the private key from Petra and use the publish command above.

### Publish (if compatible) 

```
 aptos move publish \
    --private-key <...> \
    --max-gas 100000 --url https://fullnode.testnet.aptoslabs.com/v1 \
    --included-artifacts none
 
```

### Add an order

```
aptos move run \
  --function-id 0xc4a97809df332af8bb20ebe1c60f47b7121648c5896f29dc37b4d2e60944e20d::market::add_order_entry \![Uploading Screen Shot 2022-12-13 at 1.11.24 PM.png…]()
  --type-args 0x1::aptos_coin::AptosCoin 0xc4a97809df332af8bb20ebe1c60f47b7121648c5896f29dc37b4d2e60944e20d::test_coins::USDF \
  --args u8:1 u8:1 u64:220000 u64:40000 string: \
  --url https://fullnode.testnet.aptoslabs.com/v1 \
  --private-key <…>   
```

## Troubleshooting 

### EPACKAGE_DEP_MISSING

```
{
  "Error": "API error: Unknown error Transaction committed on chain, but failed execution: Move abort in 0x1::code: EPACKAGE_DEP_MISSING(0x60005): Dependency could not be resolved to any published package."
}
```

Probably something happening in `Move.toml`. 

1. Check that you're not mixing `testnet` vs. `devent`, and that you're consistent i.e. if you're publishing to testnet, all dependencies must be on testnet as well.
2. Make sure that any custom dependencies are published to testnet / devnet. 


### EMODULE_MISSING

```
{
  "Error": "API error: Unknown error Transaction committed on chain, but failed execution: Move abort in 0x1::code: EMODULE_MISSING(0x4): Cannot delete a module that was published in the same package"
}
```

Contracts are upgradable, but they are [heavily restricted](https://aptos.dev/guides/move-guides/upgrading-move-code/). So if you deleted a public function or a module, you will need to publish to a new account. See **Publish to New Account** above.
