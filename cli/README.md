# Ferum CLI

1. Create a new profile: `ts-node src/index.ts create-profile -n ferum` (you can name it whatever).

1. Publish ferum modules: `ts-node src/index.ts publish-ferum -m ../contract`.

1. Create test coins: `ts-node src/index.ts create-test-coins`.

1. Initialize ferum: `ts-node src/index.ts init-coins`.

1. Create a market: `ts-node src/index.ts init-market -ic FMA -qc FMB -id 3 -qd 3`.

1. Play with the commands below:

## Commands

### Add Limit Order

```terminal
ts-node src/index.ts add-limit-order -ic FMA -qc FMB -p 2000 -q 1000 -s sell
```

### Add Market Order

```terminal
ts-node src/index.ts add-market-order -ic FMA -qc FMB -c 2000 -q 1000 -s sell
```

### Cancel Order

```terminal
ts-node lib/index.js cancel-order -ic FMA -qc FMB -id 12345
```

### Create Test Coins

```terminal
ts-node src/index.ts create-test-coin
```

### Check Test Coin Balance

```
ts-node src/index.ts test-coin-balances
```

### Init Market

```terminal
ts-node src/index.ts init-market -ic FMA -qc FMB -id 3 -qd 3
```

### Other Commands

See `ts-node src/index.ts --help` for a full list of commands.



