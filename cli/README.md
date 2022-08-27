# Ferum CLI

1. Set APTOS_KEY env var to your private key `export APTOS_KEY="55679798d5e00e0330b33259181422f1b1039adce44a930a1fde039e272bede7"`

2. Run `tsc --watch` from `ferum-cli` directory to continuously build.

3. Play with the commands below:

## Commands

### Create Test Coins

```terminal
ts-node src/index.ts create-test-coin -m ~/Desktop/ferrum.xyz/build/ferum/bytecode_modules/test_coin.mv -cn test_coin::Test_Coin
```

### Check Test Coin Balances

```terminal
node lib/index.js test-coin-balances
```

### Init Ferum

```terminal
node lib/index.js init-ferum
```

### Init Order Book

```terminal
node lib/index.js init-orderbook -ic 0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f::test_coin::TestCoin -qc 0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f::test_coin::TestCoin
```

### Add Limit Order

```terminal
node lib/index.js add-limit-order -ic 0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f::test_coin::TestCoin -qc 0x1::aptos_coin::AptosCoin -s buy -p 100 -q 10
```

### Add Market Order

```terminal
node lib/index.js add-market-order -ic 0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f::test_coin::TestCoin -qc 0x1::aptos_coin::AptosCoin -s buy -q 100 -c 100
```

### Cancel Order

```terminal
node lib/index.js cancel-order -id 12345
```
