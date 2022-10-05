// Has methods to perform calculations for AMM operations. It is assumed that LP coins have the same number of
// decimal places ts FixedPoint64.
module ferum::calculator {
    use ferum_std::fixed_point_64::{FixedPoint64,
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
    };

    //
    // Errors
    //

    // Calculator errors reserve [100, 199].

    const ERR_INVALID_X_SUPPLY: u64 = 100;
    const ERR_INVALID_Y_SUPPLY: u64 = 101;
    const ERR_INVALID_X_AMT: u64 = 102;
    const ERR_INVALID_Y_AMT: u64 = 103;
    const ERR_INVALID_LP_TOKEN_AMT: u64 = 104;
    const ERR_INVALID_LP_TOKEN_SUPPLY: u64 = 105;
    const ERR_DEPOSIT_PRECISION_LOSS: u64 = 106;
    const ERR_INIT_WITH_SINGLE_ASSET_DEPOSIT: u64 = 107;

    // Initial amount of pool tokens for swap contract, hard-coded to something
    // "sensible" given a maximum of u128 (similar to the spl token program).
    // Note that on Ethereum, Uniswap uses the geometric mean of all provided
    // input amounts, and Balancer uses 100 * 10 ^ 18.
    //
    // We use 100 * 10 ^ 10.
    const INITIAL_LP_SUPPLY: u128 = 1000000000000;

    // Returns price of Y in terms of X. For example if the amount of X is 100 and the amount of Y is 20, then each Y
    // is worth 5 X.
    public fun price(ySupply: FixedPoint64, xSupply: FixedPoint64, xDecimals: u8): FixedPoint64 {
        trunc_to_decimals(divide_round_up(xSupply, ySupply), xDecimals)
    }

    // Returns how many lp coins to mint and fees charged in terms of X and Y.
    // Any rounding errors are made in favor of the pool.
    //
    // If the ratio of X/Y being supplied doesn't match the ratio of X/Y in the pool,
    // only part of the provided coin is deposited while the rest is returned.
    //
    // Returns: (lpCoinsMinted, xCoinsNotUsed, yCoinsNotUsed, xCoinFee, yCoinFee)
    public fun deposit_multi_asset(
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
    public fun withdraw_multi_asset(
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
    // Y in terms of X.
    //
    // Charges a fee in token X before performing the swap. Any rounding error is made in favor of the pool.
    //
    // Returns: (yCoinsOut, feeAmt, unusedXCoin).
    public fun swap(
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
                unusedXCoin = sub(xCoinsIn, maxXCoinIn);
                xCoinsIn = maxXCoinIn;
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
