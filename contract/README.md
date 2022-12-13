# Ferum Contracts

Easiest way to interact with Ferum is through the Aptos CLI, and the easiest way to get the CLI is by downloading the latest & greatest 

### Add an order

```
aptos move run \
  --function-id 0xc4a97809df332af8bb20ebe1c60f47b7121648c5896f29dc37b4d2e60944e20d::market::add_order_entry \
  --type-args 0x1::aptos_coin::AptosCoin 0xc4a97809df332af8bb20ebe1c60f47b7121648c5896f29dc37b4d2e60944e20d::test_coins::USDF \
  --args u8:1 u8:1 u64:220000 u64:40000 string: \
  --url https://fullnode.testnet.aptoslabs.com/v1 \
  --private-key <…>   
```
