module ferum::pool {
    use ferum_std::fixed_point_64::{
        FixedPoint64,
        sub,
        add,
        multiply_round_up,
        multiply_trunc,
        divide_trunc,
        divide_round_up,
        zero,
        is_zero,
        new_u128,
        gt,
        lte,
        eq,
        min,
        trunc_to_decimals,
        round_up_to_decimals,
        from_u128,
        from_u64,
        to_u64_round_up,
        to_u64_trunc,
    };
    use aptos_framework::account::SignerCapability;
    use std::signer::address_of;
    use std::bcs;
    use std::vector;
    use aptos_framework::account;
    use aptos_std::type_info;
    use ferum::admin;
    use ferum::market;
    use aptos_framework::coin;
    use std::string;
    use ferum::lp_coin::FerumLP;
    use aptos_framework::coin::{BurnCapability, MintCapability};

    //
    // Errors
    //

    // Pool errors reserve [100, 199].

    const ERR_INVALID_X_SUPPLY: u64 = 100;
    const ERR_INVALID_Y_SUPPLY: u64 = 101;
    const ERR_INVALID_X_AMT: u64 = 102;
    const ERR_INVALID_Y_AMT: u64 = 103;
    const ERR_INVALID_LP_TOKEN_AMT: u64 = 104;
    const ERR_INVALID_LP_TOKEN_SUPPLY: u64 = 105;
    const ERR_DEPOSIT_PRECISION_LOSS: u64 = 106;
    const ERR_INIT_WITH_SINGLE_ASSET_DEPOSIT: u64 = 107;
    const ERR_NOT_ALLOWED: u64 = 108;
    const ERR_POOL_EXISTS: u64 = 109;
    const ERR_POOL_DOES_NOT_EXIST: u64 = 110;
    const ERR_INVALID_POOL_TYPE: u64 = 111;
    const ERR_INVALID_COIN_DECIMALS: u64 = 112;

    //
    // Constants
    //

    // Initial amount of pool tokens for swap contract, hard-coded to something
    // "sensible" given a maximum of u128 (similar to the spl token program).
    // Note that on Ethereum, Uniswap uses the geometric mean of all provided
    // input amounts, and Balancer uses 100 * 10 ^ 18.
    //
    // We use 100 * 10 ^ 10.
    const INITIAL_LP_SUPPLY: u128 = 1000000000000;
    const POOL_RESOURCE_ACCOUNT_SEED: vector<u8> = b"ferum::pool::resource_account_seed";
    const MAX_COIN_DECIMALS: u8 = 10;

    //
    // Enums
    //

    const POOL_TYPE_CONSTANT_PRODUCT: u8 = 1;

    const SIDE_SELL: u8 = 1;
    const SIDE_BUY: u8 = 2;

    const TYPE_RESTING: u8 = 1;

    //
    // Structs
    //

    struct PoolTypeConstantProduct {}

    struct Pool<phantom I, phantom Q, phantom T> has key {
        iSupply: FixedPoint64,
        qSupply: FixedPoint64,

        // Signer capability used to create transactions on behalf of this pool.
        signerCap: SignerCapability,
        // Enum representing which pool type this is.
        poolType: u8,
        // Total supply of LP coins.
        lpCoinSupply: FixedPoint64,
        // LP coins burn capability.
        burnCap: BurnCapability<FerumLP<I, Q, T>>,
        // LP coins mint capability.
        mintCap: MintCapability<FerumLP<I, Q, T>>,
    }

    //
    // Entry functions
    //

    public entry fun create_pool<I, Q, T>(signer: &signer) {
        // For now, pools can only be created by Ferum. This will be opened up to be permissionless soon.

        let signerAddr = address_of(signer);
        assert!(signerAddr == @ferum, ERR_NOT_ALLOWED);
        assert!(!exists<Pool<I, Q, T>>(@ferum), ERR_POOL_EXISTS);
        admin::assert_market_inited<I, Q>();

        let seed = bcs::to_bytes(&@ferum);
        vector::append(&mut seed, POOL_RESOURCE_ACCOUNT_SEED);
        let (_, signerCap) = account::create_resource_account(signer, seed);

        let iSymbol = coin::symbol<I>();
        let qSymbol = coin::symbol<Q>();
        let lpCoinName = string::utf8(b"FerumLP ");
        string::append(&mut lpCoinName, iSymbol);
        string::append_utf8(&mut lpCoinName, b"/");
        string::append(&mut lpCoinName, qSymbol);
        let lpCoinSymbol = string::utf8(b"FLP-");
        string::append(&mut lpCoinName, iSymbol);
        string::append_utf8(&mut lpCoinName, b"-");
        string::append(&mut lpCoinName, qSymbol);
        let (
            burnCap,
            freezeCap,
            mintCap,
        ) = coin::initialize<FerumLP<I, Q, T>>(signer, lpCoinName, lpCoinSymbol, 8, false);
        coin::destroy_freeze_cap(freezeCap);

        let typeInfo = type_info::type_of<T>();
        let poolType = if (typeInfo == type_info::type_of<PoolTypeConstantProduct>()) {
            POOL_TYPE_CONSTANT_PRODUCT
        } else {
            abort ERR_INVALID_POOL_TYPE
        };

        move_to(signer, Pool<I, Q, T>{
            iSupply: zero(),
            qSupply: zero(),
            signerCap,
            poolType,
            lpCoinSupply: zero(),
            burnCap,
            mintCap,
        })
    }

    public entry fun deposit<I, Q, T>(signer: &signer, coinIAmt: u64, coinQAmt: u64) acquires Pool {
        validate_pool<I, Q, T>();
        let signerAddr = address_of(signer);

        let pool = borrow_global_mut<Pool<I, Q, T>>(@ferum);
        let poolSigner = &account::create_signer_with_capability(&pool.signerCap);
        let poolSignerAddress = address_of(poolSigner);
        let (iDecimals, qDecimals) = market::get_market_decimals<I, Q>();

        let coinIAmtFP = from_u64(coinIAmt, iDecimals);
        let coinQAmtFP = from_u64(coinQAmt, iDecimals);

        let (lpCoinsToMint, unusedICoin, unusedQCoin) = deposit_multi_asset(
            pool.lpCoinSupply,
            pool.iSupply,
            coinIAmtFP,
            iDecimals,
            pool.qSupply,
            coinQAmtFP,
            qDecimals,
        );
        let coinIToWithdraw = sub(coinIAmtFP, unusedICoin);
        let coinQToWithdraw = sub(coinIAmtFP, unusedQCoin);

        pool.iSupply = add(pool.iSupply, coinIToWithdraw);
        pool.qSupply = add(pool.qSupply, coinQToWithdraw);
        pool.lpCoinSupply = add(pool.lpCoinSupply, lpCoinsToMint);

        coin::transfer<I>(signer, poolSignerAddress, to_u64_round_up(coinIToWithdraw, coin::decimals<I>()));
        coin::transfer<Q>(signer, poolSignerAddress, to_u64_round_up(coinQToWithdraw, coin::decimals<Q>()));

        let lpCoins = coin::mint(
            to_u64_trunc(lpCoinsToMint, coin::decimals<FerumLP<I, Q, T>>()),
            &pool.mintCap,
        );
        coin::deposit(signerAddr, lpCoins);
    }

    public entry fun withdraw<I, Q, T>(signer: &signer, lpCoinsToBurn: u64) acquires Pool {
        validate_pool<I, Q, T>();
        let signerAddr = address_of(signer);

        let pool =borrow_global_mut<Pool<I, Q, T>>(@ferum);
        let poolSigner = &account::create_signer_with_capability(&pool.signerCap);
        let (iDecimals, qDecimals) = market::get_market_decimals<I, Q>();

        let lpCoinsToBurnFP = from_u64(lpCoinsToBurn, coin::decimals<FerumLP<I, Q, T>>());

        let (iCoinsOut, qCoinsOut) = withdraw_multi_asset(
            lpCoinsToBurnFP,
            pool.lpCoinSupply,
            pool.iSupply,
            iDecimals,
            pool.qSupply,
            qDecimals,
        );

        pool.iSupply = sub(pool.iSupply, iCoinsOut);
        pool.qSupply = sub(pool.qSupply, qCoinsOut);
        pool.lpCoinSupply = sub(pool.lpCoinSupply, lpCoinsToBurnFP);

        coin::transfer<I>(poolSigner, signerAddr, to_u64_trunc(iCoinsOut, coin::decimals<I>()));
        coin::transfer<Q>(poolSigner, signerAddr, to_u64_trunc(qCoinsOut, coin::decimals<Q>()));

        coin::burn(coin::withdraw(signer, lpCoinsToBurn), &pool.burnCap);
    }

    public entry fun rebalance<I, Q, T>(_: &signer) acquires Pool {
        validate_pool<I, Q, T>();
        let pool = borrow_global<Pool<I, Q, T>>(@ferum);
        let poolSigner = &account::create_signer_with_capability(&pool.signerCap);
        let (iDecimals, qDecimals) = market::get_market_decimals<I, Q>();

        // First cancel all orders.
        market::cancel_all_orders_for_owner_entry<I, Q>(poolSigner);

        // This next bit of code was in a conditional branching off of the pool type. There's a move verifier bug
        // that causes a VM error when a loop is inside a conditional: https://github.com/move-language/move/issues/496.
        // So removing that for now since we don't even have more than one pool type.

        // Replace orders according to the constant product price function.
        // We simlulate the constant product oricing function by returning 20 price points along the constant
        // product price curve. The first 10 are buys, the last 10 are sells. Prices are in increasing order.
        let tenPercent = from_u64(10, 2);
        let buyQty = trunc_to_decimals(multiply_trunc(pool.qSupply, tenPercent), qDecimals);
        let sellQty = trunc_to_decimals(multiply_trunc(pool.iSupply, tenPercent), iDecimals);
        let clientOrderID = string::utf8(b"ferum_constant_product_pool");

        let pricePoints = vector<FixedPoint64>[
            // Buys Begin.
            // Price after swap 9% of I supply.
            price_after_swap(pool.iSupply, pool.qSupply, multiply_trunc(pool.iSupply, from_u128(9, 2))),
            // Price after swap 8% of I supply.
            price_after_swap(pool.iSupply, pool.qSupply, multiply_trunc(pool.iSupply, from_u128(8, 2))),
            // Price after swap 7% of I supply.
            price_after_swap(pool.iSupply, pool.qSupply, multiply_trunc(pool.iSupply, from_u128(7, 2))),
            // Price after swap 6% of I supply.
            price_after_swap(pool.iSupply, pool.qSupply, multiply_trunc(pool.iSupply, from_u128(6, 2))),
            // Price after swap 5% of I supply.
            price_after_swap(pool.iSupply, pool.qSupply, multiply_trunc(pool.iSupply, from_u128(5, 2))),
            // Price after swap 4% of I supply.
            price_after_swap(pool.iSupply, pool.qSupply, multiply_trunc(pool.iSupply, from_u128(4, 2))),
            // Price after swap 3% of I supply.
            price_after_swap(pool.iSupply, pool.qSupply, multiply_trunc(pool.iSupply, from_u128(3, 2))),
            // Price after swap 2% of I supply.
            price_after_swap(pool.iSupply, pool.qSupply, multiply_trunc(pool.iSupply, from_u128(2, 2))),
            // Price after swap 1% of I supply.
            price_after_swap(pool.iSupply, pool.qSupply, multiply_trunc(pool.iSupply, from_u128(1, 2))),
            // Price after swap 0.1% of I supply.
            price_after_swap(pool.iSupply, pool.qSupply, multiply_trunc(pool.iSupply, from_u128(1, 3))),
            // Sells Begin.
            // Price after swap 0.1% of Q supply.
            price_after_swap(pool.qSupply, pool.iSupply, multiply_trunc(pool.qSupply, from_u128(1, 3))),
            // Price after swap 1% of Q supply.
            price_after_swap(pool.qSupply, pool.iSupply, multiply_trunc(pool.qSupply, from_u128(1, 2))),
            // Price after swap 2% of Q supply.
            price_after_swap(pool.qSupply, pool.iSupply, multiply_trunc(pool.qSupply, from_u128(2, 2))),
            // Price after swap 3% of Q supply.
            price_after_swap(pool.qSupply, pool.iSupply, multiply_trunc(pool.qSupply, from_u128(3, 2))),
            // Price after swap 4% of Q supply.
            price_after_swap(pool.qSupply, pool.iSupply, multiply_trunc(pool.qSupply, from_u128(4, 2))),
            // Price after swap 5% of Q supply.
            price_after_swap(pool.qSupply, pool.iSupply, multiply_trunc(pool.qSupply, from_u128(5, 2))),
            // Price after swap 6% of Q supply.
            price_after_swap(pool.qSupply, pool.iSupply, multiply_trunc(pool.qSupply, from_u128(6, 2))),
            // Price after swap 7% of Q supply.
            price_after_swap(pool.qSupply, pool.iSupply, multiply_trunc(pool.qSupply, from_u128(7, 2))),
            // Price after swap 8% of Q supply.
            price_after_swap(pool.qSupply, pool.iSupply, multiply_trunc(pool.qSupply, from_u128(8, 2))),
            // Price after swap 9% of Q supply.
            price_after_swap(pool.qSupply, pool.iSupply, multiply_trunc(pool.qSupply, from_u128(9, 2))),
        ];
        let i = 0;
        while (i < 20) {
            let price = vector::pop_back(&mut pricePoints);
            let (side, qty) = if (i < 10) {
                (SIDE_SELL, sellQty)
            } else {
                (SIDE_BUY, buyQty)
            };
            // TODO: should use POST orders.
            market::add_order<I, Q>(poolSigner, side, TYPE_RESTING, price, qty, clientOrderID);
            i = i + 1;
        };

        // TODO: reward signer with FER.
    }

    //
    // Private helpers.
    //

    fun validate_pool<I, Q, T>() {
        assert!(exists<Pool<I, Q, T>>(@ferum), ERR_POOL_DOES_NOT_EXIST);
        // Also validate that the corresponding market exists.
        admin::assert_market_inited<I, Q>();
    }

    // Simulates a swap and returns the price (X in terms of Y) of the pool after the swap.
    fun price_after_swap(xSupply: FixedPoint64, ySupply: FixedPoint64, xCoinsIn: FixedPoint64): FixedPoint64 {
        let k = multiply_round_up(xSupply, ySupply);
        let newXSupply = add(xSupply, xCoinsIn);
        let yCoinOut = sub(
            ySupply,
            divide_round_up(k, newXSupply),
        );
        let newYSupply = sub(ySupply, yCoinOut);
        divide_trunc(
            newYSupply,
            newXSupply
        )
    }

    // Returns how many lp coins to mint and fees charged in terms of X and Y.
    // Any rounding errors are made in favor of the pool.
    //
    // If the ratio of X/Y being supplied doesn't match the ratio of X/Y in the pool,
    // only part of the provided coin is deposited while the rest is returned.
    //
    // Returns: (lpCoinsMinted, xCoinsNotUsed, yCoinsNotUsed)
    fun deposit_multi_asset(
        currentLPCoinSupply: FixedPoint64,
        xSupply: FixedPoint64,
        xCoinAmt: FixedPoint64,
        xDecimals: u8,
        ySupply: FixedPoint64,
        yCoinAmt: FixedPoint64,
        yDecimals: u8,
    ): (FixedPoint64, FixedPoint64, FixedPoint64) {
        let zero = zero();

        if (is_zero(currentLPCoinSupply)) {
            // Can't initialize using a single asset.
            assert!(!is_zero(xCoinAmt) && !is_zero(yCoinAmt), ERR_INIT_WITH_SINGLE_ASSET_DEPOSIT);
            // Return a constant which the pool is being initialized.
            return (new_u128(INITIAL_LP_SUPPLY), zero, zero)
        };

        // Some input parameter checks.
        assert!(!is_zero(xSupply), ERR_INVALID_X_SUPPLY);
        assert!(!is_zero(ySupply), ERR_INVALID_Y_SUPPLY);
        assert!(!is_zero(xCoinAmt), ERR_INVALID_X_AMT);
        assert!(!is_zero(yCoinAmt), ERR_INVALID_Y_AMT);

        let xRatio = divide_trunc(xCoinAmt, xSupply);
        let yRatio = divide_trunc(yCoinAmt, ySupply);
        let (xUsed, yUsed) = if (gt(xRatio, yRatio)) {
            (multiply_round_up(yRatio, xSupply), yCoinAmt)
        } else {
            (xCoinAmt, multiply_round_up(xRatio, ySupply))
        };

        let xToReturn = sub(xCoinAmt, xUsed);
        let yToReturn = sub(yCoinAmt, yUsed);

        let xLPCoins =
            divide_trunc(
                multiply_trunc(xUsed, currentLPCoinSupply),
                xSupply,
            );

        let yLPCoins =
            divide_trunc(
                multiply_trunc(yUsed, currentLPCoinSupply),
                ySupply,
            );

        let lpCoinsToMint = min(yLPCoins, xLPCoins);
        assert!(!is_zero(lpCoinsToMint), ERR_DEPOSIT_PRECISION_LOSS);

        (
            lpCoinsToMint,
            trunc_to_decimals(xToReturn, xDecimals),
            trunc_to_decimals(yToReturn, yDecimals),
        )
    }

    // Returns how many of the underlying assets to give back to the user for the given amount
    // of LP coins. Any rounding errors are made in favor of the pool.
    //
    // Returns: (xCoinsOut, yCoinsOut)
    fun withdraw_multi_asset(
        lpCoinsToBurn: FixedPoint64,
        currentLPCoinSupply: FixedPoint64,
        xSupply: FixedPoint64,
        xDecimals: u8,
        ySupply: FixedPoint64,
        yDecimals: u8,
    ): (FixedPoint64, FixedPoint64) {
        // Some input parameter checks.
        assert!(!is_zero(xSupply), ERR_INVALID_X_SUPPLY);
        assert!(!is_zero(ySupply), ERR_INVALID_Y_SUPPLY);
        assert!(!is_zero(lpCoinsToBurn), ERR_INVALID_LP_TOKEN_AMT);
        assert!(!is_zero(currentLPCoinSupply), ERR_INVALID_LP_TOKEN_SUPPLY);

        let xTokens =
            divide_trunc(
                multiply_trunc(xSupply, lpCoinsToBurn),
                currentLPCoinSupply,
            );
        let yTokens =
            divide_trunc(
                multiply_trunc(ySupply, lpCoinsToBurn),
                currentLPCoinSupply,
            );

        (trunc_to_decimals(xTokens, xDecimals), trunc_to_decimals(yTokens, yDecimals))
    }

    // Calculates the amount of Y produced when swapping for the specified X amount.
    // Only swaps up to n amount that will still satisfy the limit price. The limit price is the price of
    // Y in terms of X. Note that the limit price may still be exceeded due to rounding but the error is limited to
    // the number of decimal places for the asset.
    //
    // Charges a fee in token X before performing the swap. Any rounding error is made in favor of the pool.
    //
    // Returns: (yCoinsOut, feeAmt, unusedXCoin).
    fun swap(
        xSupply: FixedPoint64,
        xCoinDecimals: u8,
        ySupply: FixedPoint64,
        yCoinDecimals: u8,
        xCoinsIn: FixedPoint64,
        swapFeeBps: FixedPoint64,
        limitPrice: FixedPoint64,
    ): (FixedPoint64, FixedPoint64, FixedPoint64) {
        let zero = zero();

        // Some input parameter checks.
        assert!(!is_zero(xSupply), ERR_INVALID_X_SUPPLY);
        assert!(!is_zero(ySupply), ERR_INVALID_Y_SUPPLY);
        assert!(!is_zero(xCoinsIn), ERR_INVALID_X_AMT);

        let unusedXCoin = zero;
        if (!is_zero(limitPrice)) {
            // Calculate the max amount that can be swapped while still ensuring the swap's
            // price is less than the limit price.
            //
            // The max quantity is determined by solving `xCoinsIn / yCoinOut <= limitPrice`,
            // where yCoinOut is given by ySupply - ((xSupply * ySupply) / (xSupply + xCoinsIn)).
            //
            // The above reduces simply to xCoinsIn <= ySupply * limitPrice - xSupply.
            let product = multiply_trunc(ySupply, limitPrice);
            if (lte(product, xSupply)) {
                return (zero, zero, zero)
            };
            let maxXCoinIn = sub(product, xSupply);
            if (gt(xCoinsIn, maxXCoinIn)) {
                unusedXCoin = round_up_to_decimals(sub(xCoinsIn, maxXCoinIn), xCoinDecimals);
                xCoinsIn = sub(xCoinsIn, unusedXCoin);
            };
        };

        let feeAmt = round_up_to_decimals(multiply_round_up(xCoinsIn, swapFeeBps), xCoinDecimals);
        xCoinsIn = sub(xCoinsIn, feeAmt);

        let k = multiply_round_up(xSupply, ySupply);
        let newXAmt = add(xSupply, xCoinsIn);
        let yCoinOut = sub(
            ySupply,
            divide_round_up(k, newXAmt),
        );
        (
            trunc_to_decimals(yCoinOut, yCoinDecimals),
            feeAmt,
            unusedXCoin,
        )
    }

    //
    // Tests
    //

    //
    // Deposit Tests
    //

    #[test]
    fun test_deposit_multi_asset_initial_even() {
        let xAmt = from_u128(100, 0);
        let yAmt = from_u128(100, 0);

        let (lpTokens, xToReturn, yToReturn) = deposit_multi_asset(
            zero(), zero(), xAmt, 10, zero(), yAmt, 10);
        assert!(is_zero(xToReturn), 0);
        assert!(is_zero(yToReturn), 0);
        assert!(
            eq(lpTokens, from_u128(INITIAL_LP_SUPPLY, 10)),
            0,
        );
    }

    #[test]
    fun test_deposit_multi_asset_initial_uneven() {
        let xAmt = from_u128(200, 0);
        let yAmt = from_u128(100, 0);

        let (lpTokens, xToReturn, yToReturn) = deposit_multi_asset(
            zero(), zero(), xAmt, 10, zero(), yAmt, 10);
        assert!(is_zero(xToReturn), 0);
        assert!(is_zero(yToReturn), 0);
        assert!(
            eq(lpTokens, from_u128(INITIAL_LP_SUPPLY, 10)),
            0,
        );
    }

    #[test]
    fun test_deposit_multi_asset_existing_pool_same_ratio() {
        let xAmt = from_u128(200, 0);
        let yAmt = from_u128(100, 0);
        let xSupply = from_u128(500, 0);
        let ySupply = from_u128(250, 0);
        let currentLPTokenSupply = from_u128(100, 0);

        let (lpTokens, xToReturn, yToReturn) = deposit_multi_asset(
            currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10);
        assert!(is_zero(xToReturn), 0);
        assert!(is_zero(yToReturn), 0);
        assert!(
            eq(lpTokens, from_u128(40, 0)),
            0,
        );
    }

    #[test]
    fun test_deposit_multi_asset_existing_pool_different_ratio_x_bound() {
        let xAmt = from_u128(100, 0);
        let yAmt = from_u128(100, 0);
        let xSupply = from_u128(500, 0);
        let ySupply = from_u128(250, 0);
        let currentLPTokenSupply = from_u128(100, 0);

        let (lpTokens, xToReturn, yToReturn) = deposit_multi_asset(
            currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10);
        assert!(is_zero(xToReturn), 0);
        assert!(eq(yToReturn, from_u128(50, 0)), 0);
        assert!(
            eq(lpTokens, from_u128(20, 0)),
            0,
        );
    }

    #[test]
    fun test_deposit_multi_asset_existing_pool_different_ratio_y_bound() {
        let xAmt = from_u128(100, 0);
        let yAmt = from_u128(30, 0);
        let xSupply = from_u128(500, 0);
        let ySupply = from_u128(250, 0);
        let currentLPTokenSupply = from_u128(100, 0);

        let (lpTokens, xToReturn, yToReturn) = deposit_multi_asset(
            currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10);
        assert!(is_zero(yToReturn), 0);
        assert!(eq(xToReturn, from_u128(40, 0)), 0);
        assert!(
            eq(lpTokens, from_u128(12, 0)),
            0,
        );
    }

    #[test]
    fun test_deposit_multi_asset_truncation() {
        let xAmt = from_u128(1, 9);
        let yAmt = from_u128(2, 9);
        let xSupply = from_u128(3, 9);
        let ySupply = from_u128(5, 9);
        let currentLPTokenSupply = from_u128(100, 0);

        let (lpTokens, xToReturn, yToReturn) = deposit_multi_asset(
            currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10);
        assert!(is_zero(xToReturn), 0);
        assert!(eq(yToReturn, from_u128(3, 10)), 0);
        assert!(
            eq(lpTokens, from_u128(333333333333, 10)),
            0,
        );
    }

    #[test]
    #[expected_failure(abort_code = 106)]
    fun test_deposit_multi_asset_precision_loss() {
        let xAmt = from_u128(1, 10);
        let yAmt = from_u128(3, 10);
        let xSupply = from_u128(5, 0);
        let ySupply = from_u128(25, 1);
        let currentLPTokenSupply = from_u128(100, 0);

        deposit_multi_asset(currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10);
    }

    //
    // Withdrawal Tests
    //

    #[test]
    fun test_withdraw_multi_asset_pool_after_init() {
        let xAmt = from_u128(250, 0);
        let yAmt = from_u128(125, 0);

        // Initialize the pool.
        let (lpCoinsMinted, _, _) = deposit_multi_asset(
            zero(), zero(), xAmt, 10, zero(), yAmt, 10);
        assert!(eq(lpCoinsMinted, from_u128(INITIAL_LP_SUPPLY, 10)), 0);

        // Swap back minted tokens for pool assets.
        let (xCoinsOut, yCoinsOut) = withdraw_multi_asset(
            lpCoinsMinted,
            from_u128(INITIAL_LP_SUPPLY, 10),
            from_u128(250, 0),
            10,
            from_u128(125, 0),
            10,
        );
        assert!(eq(xCoinsOut, xAmt), 0);
        assert!(eq(yCoinsOut, yAmt), 0);
    }

    #[test]
    fun test_withdraw_multi_asset_pool() {
        let xAmt = from_u128(250, 0);
        let yAmt = from_u128(125, 0);
        let xSupply = from_u128(500, 0);
        let ySupply = from_u128(250, 0);
        let currentLPTokenSupply = from_u128(100, 0);

        // Get some LP tokens.
        let (lpCoinsMinted, _, _) = deposit_multi_asset(
            currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10);
        assert!(eq(lpCoinsMinted, from_u128(50, 0)), 0);

        // Swap back minted tokens for pool assets.
        let (xCoinsOut, yCoinsOut) = withdraw_multi_asset(
            lpCoinsMinted,
            from_u128(150, 0),
            from_u128(750, 0),
            10,
            from_u128(375, 0),
            10,
        );
        assert!(eq(xCoinsOut, xAmt), 0);
        assert!(eq(yCoinsOut, yAmt), 0);
    }

    //
    // Swap Tests
    //

    #[test]
    fun test_pool_swap() {
        let fee = from_u128(1, 4);
        let xAmt = from_u128(250, 0);
        let xSupply = from_u128(500, 0);
        let ySupply = from_u128(250, 0);
        let limitPrice = from_u128(100, 0);

        let (yCoinsOut, feeAmt, unusedXCoins) = swap(
            xSupply,
            10,
            ySupply,
            10,
            xAmt,
            fee,
            limitPrice,
        );
        assert!(eq(feeAmt, from_u128(25, 3)), 0);
        assert!(eq(unusedXCoins, zero()), 0);
        assert!(eq(yCoinsOut, from_u128(833277775925, 10)), 0);
    }

    #[test]
    fun test_pool_swap_no_fee() {
        let fee = zero();
        let xAmt = from_u128(250, 0);
        let xSupply = from_u128(500, 0);
        let ySupply = from_u128(250, 0);
        let limitPrice = from_u128(100, 0);

        let (yCoinsOut, feeAmt, unusedXCoins) = swap(
            xSupply,
            10,
            ySupply,
            10,
            xAmt,
            fee,
            limitPrice,
        );
        assert!(eq(feeAmt, zero()), 0);
        assert!(eq(unusedXCoins, zero()), 0);
        assert!(eq(yCoinsOut, from_u128(833333333333, 10)), 0);
    }

    #[test]
    fun test_pool_swap_limit_price_no_fee() {
        let fee = zero();
        let xAmt = from_u128(250, 0);
        let xSupply = from_u128(500, 0);
        let ySupply = from_u128(250, 0);
        let limitPrice = from_u128(25, 1); // A max price of 2.5 X for 1 Y

        let (yCoinsOut, feeAmt, unusedXCoins) = swap(
            xSupply,
            10,
            ySupply,
            10,
            xAmt,
            fee,
            limitPrice,
        );
        assert!(eq(feeAmt, zero()), 0);
        assert!(eq(unusedXCoins, from_u128(125, 0)), 0);
        assert!(eq(yCoinsOut, from_u128(50, 0)), 0);
    }

    #[test]
    fun test_pool_swap_limit_price_fee() {
        let fee = zero();
        let xAmt = from_u128(250, 0);
        let xSupply = from_u128(500, 0);
        let ySupply = from_u128(250, 0);
        let limitPrice = from_u128(25, 1); // A max price of 2.5 X for 1 Y

        let (yCoinsOut, feeAmt, unusedXCoins) = swap(
            xSupply,
            10,
            ySupply,
            10,
            xAmt,
            fee,
            limitPrice,
        );
        assert!(eq(feeAmt, zero()), 0);
        assert!(eq(unusedXCoins, from_u128(125, 0)), 0);
        assert!(eq(yCoinsOut, from_u128(50, 0)), 0);
    }
}
