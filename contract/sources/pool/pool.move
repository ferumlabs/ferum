module ferum::pool {
    use ferum_std::fixed_point_64::{
        FixedPoint64, sub, add, multiply_round_up, multiply_trunc, divide_trunc, zero, is_zero,
        new_u128, gt, gte, lt, lte, min, trunc_to_decimals, from_u128, from_u64, to_u64_round_up, to_u64_trunc, value,
        sqrt_approx};
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
    use ferum_std::math;

    #[test_only]
    use ferum_std::fixed_point_64::{eq, to_u64, one};
    #[test_only]
    use ferum::coin_test_helpers::{create_fake_coins, register_fma_fmb, FMA, FMB, deposit_fma};
    #[test_only]
    use aptos_framework::account::{get_signer_capability_address, create_signer_with_capability};
    #[test_only]
    use ferum::market::{get_order_price, get_order_side, get_order_original_qty};

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
    const ERR_INIT_WITH_ZERO_ASSET: u64 = 107;
    const ERR_NOT_ALLOWED: u64 = 108;
    const ERR_POOL_EXISTS: u64 = 109;
    const ERR_POOL_DOES_NOT_EXIST: u64 = 110;
    const ERR_INVALID_POOL_TYPE: u64 = 111;
    const ERR_INVALID_COIN_DECIMALS: u64 = 112;
    const ERR_MAX_POOL_COIN_AMT_REACHED: u64 = 113;
    const ERR_MAX_LP_COIN_AMT_REACHED: u64 = 114;
    const ERR_UNSUPPORTED_DECIMAL_PLACES: u64 = 115;
    const ERR_INVALID_DELTA_TARGET: u64 = 116;

    //
    // Constants
    //

    // Exponent to add the saem number of decimal places that a FixedPoint64 has.
    const FP_EXP: u128 = 10000000000;
    // Initial amount of pool tokens for swap contract, hard-coded to something
    // "sensible" given a maximum of u128 (similar to the spl token program).
    // Note that on Ethereum, Uniswap uses the geometric mean of all provided
    // input amounts, and Balancer uses 100 * 10 ^ 18.
    //
    // We use 100 * 10 ^ 10.
    const INITIAL_LP_SUPPLY: u128 = 1000000000000;
    const POOL_RESOURCE_ACCOUNT_SEED: vector<u8> = b"ferum::pool::resource_account_seed";
    // The max number of coins a pool can store are chosen to ensure that all the operations when calculating price
    // points can be performed. ~1 billion coins can be in a pool, which seems like enough.
    const MAX_POOL_COIN_COUNT: u128 = 1044674407;
    // The max number of LP tokens that can exist are chosen to ensure that all the operations when calculating price
    // points can be performed. ~1 billion coins can be in a pool, which seems like enough.
    const MAX_LP_COINS: u128 = 1044674407;
    // Number of decimals the LP coin has.
    const LP_COIN_DECIMALS: u8 = 8;
    // The sum of the assets' decimal places must be less than this number to ensure all arithmetic operations can
    // take place.
    const MAX_SUM_DECIMAL_PLACES: u8 = 10;

    //
    // Enums
    //

    const POOL_TYPE_CONSTANT_PRODUCT: u8 = 1;
    const POOL_TYPE_STABLE_SWAP: u8 = 2;

    const SIDE_SELL: u8 = 1;
    const SIDE_BUY: u8 = 2;

    const TYPE_RESTING: u8 = 1;

    //
    // Structs
    //

    struct ConstantProduct {}

    struct StableSwap {}

    struct Pool<phantom I, phantom Q, phantom T> has key {
        // Supply of the I coin in the pool.
        // Max value is MAX_POOL_COIN_COUNT.
        // Min non zero value is from_u128(1, iDecimals).
        iSupply: FixedPoint64,
        // Supply of the Q coin in the pool.
        // Max value is MAX_POOL_COIN_COUNT.
        // Min non zero value is from_u128(1, qDecimals).
        qSupply: FixedPoint64,
        // Signer capability used to create transactions on behalf of this pool.
        signerCap: SignerCapability,
        // Enum representing which pool type this is.
        type: u8,
        // Total supply of LP coins.
        // Max value is MAX_LP_COINS.
        // Min non zero value is from_u128(1, LP_COIN_DECIMALS).
        lpCoinSupply: FixedPoint64,
        // LP coins burn capability.
        burnCap: BurnCapability<FerumLP<I, Q, T>>,
        // LP coins mint capability.
        mintCap: MintCapability<FerumLP<I, Q, T>>,
    }

    //
    // Entry functions
    //

    public entry fun create_pool_entry<I, Q, T>(signer: &signer) {
        // For now, pools can only be created by Ferum. This will be opened up to be permissionless soon.

        let signerAddr = address_of(signer);
        assert!(signerAddr == @ferum, ERR_NOT_ALLOWED);
        assert!(!exists<Pool<I, Q, T>>(@ferum), ERR_POOL_EXISTS);
        admin::assert_market_inited<I, Q>();

        let (iDecimals, qDecimals) = market::get_market_decimals<I, Q>();
        validate_decimal_places(iDecimals, qDecimals);

        let typeInfo = type_info::type_of<T>();
        let poolType = if (typeInfo == type_info::type_of<ConstantProduct>()) {
            POOL_TYPE_CONSTANT_PRODUCT
        } else if (typeInfo == type_info::type_of<StableSwap>()) {
            POOL_TYPE_STABLE_SWAP
        } else {
            abort ERR_INVALID_POOL_TYPE
        };

        let seed = bcs::to_bytes(&@ferum);
        vector::append(&mut seed, POOL_RESOURCE_ACCOUNT_SEED);
        vector::append(&mut seed, bcs::to_bytes(&poolType));
        let (poolSigner, poolSsignerCap) = account::create_resource_account(signer, seed);
        coin::register<I>(&poolSigner);
        coin::register<Q>(&poolSigner);

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
        ) = coin::initialize<FerumLP<I, Q, T>>(signer, lpCoinName, lpCoinSymbol, LP_COIN_DECIMALS, false);
        coin::destroy_freeze_cap(freezeCap);

        move_to(signer, Pool<I, Q, T>{
            iSupply: zero(),
            qSupply: zero(),
            signerCap: poolSsignerCap,
            type: poolType,
            lpCoinSupply: zero(),
            burnCap,
            mintCap,
        })
    }

    public entry fun deposit_entry<I, Q, T>(signer: &signer, coinIAmt: u64, coinQAmt: u64) acquires Pool {
        validate_pool<I, Q, T>();
        let signerAddr = address_of(signer);

        let pool = borrow_global_mut<Pool<I, Q, T>>(@ferum);
        let poolSigner = &account::create_signer_with_capability(&pool.signerCap);
        let poolSignerAddress = address_of(poolSigner);
        let (iDecimals, qDecimals) = market::get_market_decimals<I, Q>();
        let lpCoinDecimals = coin::decimals<FerumLP<I, Q, T>>();

        let coinIAmtFP = from_u64(coinIAmt, iDecimals);
        let coinQAmtFP = from_u64(coinQAmt, qDecimals);

        let (lpCoinsToMint, unusedICoin, unusedQCoin) = deposit_multi_asset(
            pool.lpCoinSupply,
            pool.iSupply,
            coinIAmtFP,
            iDecimals,
            pool.qSupply,
            coinQAmtFP,
            qDecimals,
            lpCoinDecimals,
        );
        let coinIToWithdraw = sub(coinIAmtFP, unusedICoin);
        let coinQToWithdraw = sub(coinQAmtFP, unusedQCoin);

        pool.iSupply = add(pool.iSupply, coinIToWithdraw);
        pool.qSupply = add(pool.qSupply, coinQToWithdraw);
        pool.lpCoinSupply = add(pool.lpCoinSupply, lpCoinsToMint);

        coin::transfer<I>(signer, poolSignerAddress, to_u64_round_up(coinIToWithdraw, coin::decimals<I>()));
        coin::transfer<Q>(signer, poolSignerAddress, to_u64_round_up(coinQToWithdraw, coin::decimals<Q>()));

        if (!coin::is_account_registered<FerumLP<I, Q, T>>(signerAddr)) {
            coin::register<FerumLP<I, Q, T>>(signer);
        };

        let lpCoins = coin::mint(
            to_u64_trunc(lpCoinsToMint, lpCoinDecimals),
            &pool.mintCap,
        );
        coin::deposit(signerAddr, lpCoins);
    }

    public entry fun withdraw_entry<I, Q, T>(signer: &signer, lpCoinsToBurn: u64) acquires Pool {
        validate_pool<I, Q, T>();
        let signerAddr = address_of(signer);

        let pool = borrow_global_mut<Pool<I, Q, T>>(@ferum);
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

    public entry fun rebalance_entry<I, Q, T>(_: &signer) acquires Pool {
        validate_pool<I, Q, T>();
        let pool = borrow_global_mut<Pool<I, Q, T>>(@ferum);
        let poolSigner = &account::create_signer_with_capability(&pool.signerCap);
        let (iDecimals, qDecimals) = market::get_market_decimals<I, Q>();

        // First cancel all orders.
        market::cancel_all_orders_for_owner_entry<I, Q>(poolSigner);

        // Reupdate the pool's supply variables.
        pool.iSupply = from_u64(coin::balance<I>(address_of(poolSigner)), coin::decimals<I>());
        pool.qSupply = from_u64(coin::balance<Q>(address_of(poolSigner)), coin::decimals<Q>());

        if (is_zero(pool.iSupply) || is_zero(pool.qSupply)) {
            // We can't place orders if there is nothing in the pool.
            return
        };

        if (pool.type == POOL_TYPE_CONSTANT_PRODUCT) {
            rebalance_constant_product(pool, poolSigner, iDecimals, qDecimals);
        } else if (pool.type == POOL_TYPE_STABLE_SWAP) {
            rebalance_stable_swap(pool, poolSigner, iDecimals, qDecimals);
        } else {
            abort ERR_INVALID_POOL_TYPE
        };

        // TODO: reward signer with FER.
    }

    //
    // Public functions.
    //

    public fun validate_decimal_places(iDecimals: u8, qDecimals: u8) {
        assert!(iDecimals + qDecimals < MAX_SUM_DECIMAL_PLACES, ERR_UNSUPPORTED_DECIMAL_PLACES);
        assert!(iDecimals + iDecimals < MAX_SUM_DECIMAL_PLACES, ERR_UNSUPPORTED_DECIMAL_PLACES);
        assert!(qDecimals + qDecimals < MAX_SUM_DECIMAL_PLACES, ERR_UNSUPPORTED_DECIMAL_PLACES);
    }

    //
    // Private helpers.
    //

    fun rebalance_constant_product<I, Q, T>(pool: &Pool<I, Q, T>, poolSigner: &signer, iDecimals: u8, qDecimals: u8) {
        // Replace orders according to the constant product price invariant: xy.
        // We simlulate the curve by returning 12 price points along the curve.
        let clientOrderID = string::utf8(b"ferum_constant_product_pool");

        // The first half is buys, the last half is sells.
        let pricePoints = get_constant_product_rebalance_prices(pool.iSupply, pool.qSupply, qDecimals);
        let length = vector::length(&pricePoints);
        let midpoint = length / 2;
        let i = 0;
        while (i < length) {
            let price = vector::pop_back(&mut pricePoints);
            if (is_zero(price)) {
                i = i + 1;
                continue
            };
            let (side, qty) = if (i >= midpoint) {
                (SIDE_SELL, get_order_qty(pool.iSupply, pool.qSupply, price, SIDE_SELL, length, iDecimals))
            } else {
                (SIDE_BUY, get_order_qty(pool.iSupply, pool.qSupply, price, SIDE_BUY, length, iDecimals))
            };
            // TODO: should use POST orders.
            market::add_order<I, Q>(poolSigner, side, TYPE_RESTING, price, qty, clientOrderID);
            i = i + 1;
        };
    }

    fun rebalance_stable_swap<I, Q, T>(pool: &Pool<I, Q, T>, poolSigner: &signer, iDecimals: u8, qDecimals: u8) {
        // Replace orders according to the the stable swap invariant: (x^3)y + x(y^3).
        // We simlulate the curve by returning 20 price points along the curve. The first 10 are buys,
        // the last 10 are sells. Prices are in increasing order.
        let tenPercent = from_u64(10, 2);
        let clientOrderID = string::utf8(b"ferum_stable_swap_pool");

        let pricePoints = get_stable_swap_prices(pool);
        let length = vector::length(&pricePoints);
        let midpoint = length / 2;
        let i = 0;
        while (i < length) {
            let nonTruncatedPrice = new_u128(vector::pop_back(&mut pricePoints));
            let price = trunc_to_decimals(nonTruncatedPrice, qDecimals);
            if (is_zero(price)) {
                i = i + 1;
                continue
            };
            let (side, qty) = if (i < midpoint) {
                // We want to sell an even proportion of I for each order.
                let sellQty = trunc_to_decimals(multiply_trunc(pool.iSupply, tenPercent), iDecimals);
                (SIDE_SELL, sellQty)
            } else {
                // We want to sell an even proportion of Q for each order. The quantity is how much I we can buy at the
                // given price.
                let qToSpend = multiply_trunc(pool.qSupply, tenPercent);
                let buyQty = trunc_to_decimals(divide_trunc(qToSpend, nonTruncatedPrice), iDecimals);
                (SIDE_BUY, buyQty)
            };
            // TODO: should use POST orders.
            market::add_order<I, Q>(poolSigner, side, TYPE_RESTING, price, qty, clientOrderID);
            i = i + 1;
        };
    }

    fun get_constant_product_rebalance_prices(iSupply: FixedPoint64, qSupply: FixedPoint64, qDecimals: u8): vector<FixedPoint64> {
        let minValue = from_u64(1, 5);

        let minTick = from_u128(1, qDecimals);
        let price = get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, iSupply, qSupply, qDecimals);
        let deltaIStep = approx_delta_i_for_constant_product_price_change(iSupply, qSupply, sub(price, minTick));
        if (lt(deltaIStep, minValue)) {
            deltaIStep = minValue;
        };
        let deltaQStep = approx_delta_q_for_constant_product_price_change(iSupply, qSupply, add(price, minTick));
        if (lt(deltaQStep, minValue)) {
            deltaIStep = minValue;
        };

        let iSteps = divide_trunc(iSupply, deltaIStep);
        let sellStep6 = multiply_trunc(multiply_trunc(iSteps, from_u128(6, 3)), deltaIStep);
        let sellStep5 = multiply_trunc(multiply_trunc(iSteps, from_u128(5, 3)), deltaIStep);
        let sellStep4 = multiply_trunc(multiply_trunc(iSteps, from_u128(4, 3)), deltaIStep);
        let sellStep3 = multiply_trunc(multiply_trunc(iSteps, from_u128(3, 3)), deltaIStep);
        let sellStep2 = multiply_trunc(multiply_trunc(iSteps, from_u128(2, 3)), deltaIStep);
        let sellStep1 = multiply_trunc(multiply_trunc(iSteps, from_u128(1, 3)), deltaIStep);

        let qSteps = divide_trunc(qSupply, deltaQStep);
        let buyStep6 = multiply_trunc(multiply_trunc(qSteps, from_u128(6, 3)), deltaQStep);
        let buyStep5 = multiply_trunc(multiply_trunc(qSteps, from_u128(5, 3)), deltaQStep);
        let buyStep4 = multiply_trunc(multiply_trunc(qSteps, from_u128(4, 3)), deltaQStep);
        let buyStep3 = multiply_trunc(multiply_trunc(qSteps, from_u128(3, 3)), deltaQStep);
        let buyStep2 = multiply_trunc(multiply_trunc(qSteps, from_u128(2, 3)), deltaQStep);
        let buyStep1 = multiply_trunc(multiply_trunc(qSteps, from_u128(1, 3)), deltaQStep);

        vector<FixedPoint64>[
            // Price after swapping ~0.6% of Q supply.
            price_after_constant_product_swap_q_to_i(iSupply, qSupply, buyStep6, qDecimals),
            // Price after swapping ~0.5% of Q supply.
            price_after_constant_product_swap_q_to_i(iSupply, qSupply, buyStep5, qDecimals),
            // Price after swapping ~0.4% of Q supply.
            price_after_constant_product_swap_q_to_i(iSupply, qSupply, buyStep4, qDecimals),
            // Price after swapping ~0.3% of Q supply.
            price_after_constant_product_swap_q_to_i(iSupply, qSupply, buyStep3, qDecimals),
            // Price after swapping ~0.2% of Q supply.
            price_after_constant_product_swap_q_to_i(iSupply, qSupply, buyStep2, qDecimals),
            // Price after swapping ~0.1% of Q supply.
            price_after_constant_product_swap_q_to_i(iSupply, qSupply, buyStep1, qDecimals),

            // Price after swapping ~0.1% of I supply.
            price_after_constant_product_swap_i_to_q(iSupply, qSupply, sellStep1, qDecimals),
            // Price after swapping ~0.2% of I supply.
            price_after_constant_product_swap_i_to_q(iSupply, qSupply, sellStep2, qDecimals),
            // Price after swapping ~0.3% of I supply.
            price_after_constant_product_swap_i_to_q(iSupply, qSupply, sellStep3, qDecimals),
            // Price after swapping ~0.4% of I supply.
            price_after_constant_product_swap_i_to_q(iSupply, qSupply, sellStep4, qDecimals),
            // Price after swapping ~0.5% of I supply.
            price_after_constant_product_swap_i_to_q(iSupply, qSupply, sellStep5, qDecimals),
            // Price after swapping ~0.6% of I supply.
            price_after_constant_product_swap_i_to_q(iSupply, qSupply, sellStep6, qDecimals),
        ]
    }

    fun get_stable_swap_prices<I, Q, T>(pool: &Pool<I, Q, T>): vector<u128> {
        let iSupply = value(pool.iSupply);
        let qSupply = value(pool.qSupply);
        vector<u128>[
            // Buys Begin.
            // Price after swap 9% of Q supply.
            price_after_stable_swap_y_to_x(iSupply, qSupply, qSupply * 9 / 100000000),
            // Price after swap 8% of Q supply.
            price_after_stable_swap_y_to_x(iSupply, qSupply, qSupply * 8 / 100000000),
            // Price after swap 7% of Q supply.
            price_after_stable_swap_y_to_x(iSupply, qSupply, qSupply * 7 / 100000000),
            // Price after swap 6% of Q supply.
            price_after_stable_swap_y_to_x(iSupply, qSupply, qSupply * 6 / 100000000),
            // Price after swap 5% of Q supply.
            price_after_stable_swap_y_to_x(iSupply, qSupply, qSupply * 5 / 100000000),
            // Price after swap 4% of Q supply.
            price_after_stable_swap_y_to_x(iSupply, qSupply, qSupply * 4 / 100000000),
            // Price after swap 3% of Q supply.
            price_after_stable_swap_y_to_x(iSupply, qSupply, qSupply * 3 / 100000000),
            // Price after swap 2% of Q supply.
            price_after_stable_swap_y_to_x(iSupply, qSupply, qSupply * 2 / 100000000),
            // Price after swap 1% of Q supply.
            price_after_stable_swap_y_to_x(iSupply, qSupply, qSupply * 1 / 100000000),
            // Price after swap 0.1% of Q supply.
            price_after_stable_swap_y_to_x(iSupply, qSupply, qSupply * 1 / 1000000000),
            // Sells Begin.
            // Price after swap 0.1% of I supply.
            price_after_stable_swap_x_to_y(iSupply, qSupply, iSupply * 1 / 1000000000),
            // Price after swap 1% of I supply.
            price_after_stable_swap_x_to_y(iSupply, qSupply, iSupply * 1 / 100000000),
            // Price after swap 2% of I supply.
            price_after_stable_swap_x_to_y(iSupply, qSupply, iSupply * 2 / 100000000),
            // Price after swap 3% of I supply.
            price_after_stable_swap_x_to_y(iSupply, qSupply, iSupply * 3 / 100000000),
            // Price after swap 4% of I supply.
            price_after_stable_swap_x_to_y(iSupply, qSupply, iSupply * 4 / 100000000),
            // Price after swap 5% of I supply.
            price_after_stable_swap_x_to_y(iSupply, qSupply, iSupply * 5 / 100000000),
            // Price after swap 6% of I supply.
            price_after_stable_swap_x_to_y(iSupply, qSupply, iSupply * 6 / 100000000),
            // Price after swap 7% of I supply.
            price_after_stable_swap_x_to_y(iSupply, qSupply, iSupply * 7 / 100000000),
            // Price after swap 8% of I supply.
            price_after_stable_swap_x_to_y(iSupply, qSupply, iSupply * 8 / 100000000),
            // Price after swap 9% of I supply.
            price_after_stable_swap_x_to_y(iSupply, qSupply, iSupply * 9 / 100000000),
        ]
    }

    fun validate_pool<I, Q, T>() {
        assert!(exists<Pool<I, Q, T>>(@ferum), ERR_POOL_DOES_NOT_EXIST);
        // Also validate that the corresponding market exists.
        admin::assert_market_inited<I, Q>();
    }

    // Computes the smallest amount of I that needs to be swapped to get at least the target price (price of I in terms of Q).
    fun approx_delta_i_for_constant_product_price_change(x: FixedPoint64, y: FixedPoint64, target: FixedPoint64): FixedPoint64 {
        let xy = multiply_trunc(x, y);
        let sqrtTerm = divide_trunc(xy, target);
        let sqrt = sqrt_approx(sqrtTerm);
        if (lt(sqrt, x)) {
            return zero()
        };
        sub(sqrt, x)
    }

    // Computes the smallest amount of Q that needs to be swapped to get at least the target price (price of I in terms of Q).
    fun approx_delta_q_for_constant_product_price_change(x: FixedPoint64, y: FixedPoint64, target: FixedPoint64): FixedPoint64 {
        let xy = multiply_trunc(x, y);
        let sqrtTerm = divide_trunc(xy, target);
        let sqrt = sqrt_approx(sqrtTerm);
        if (lt(x, sqrt)) {
            return zero()
        };
        sub(x, sqrt)
    }

    // Simulates a constant product swap from I to Q and returns the price (I in terms of Q) of the pool after the swap.
    // The max allowed to swap is MAX_POOL_COIN_COUNT. Supply amounts must be > 0.
    // Returns 0 if inputs are invalid.
    fun price_after_constant_product_swap_i_to_q(
        i: FixedPoint64,
        q: FixedPoint64,
        iDelta: FixedPoint64,
        qDecimals: u8,
    ): FixedPoint64 {
        let maxSwap = from_u128(MAX_POOL_COIN_COUNT, 0);
        if (is_zero(i) || is_zero(q) || gt(iDelta, maxSwap)) {
            return zero()
        };
        let (newX, newY) = supply_after_constant_product_swap(i, q, iDelta);
        get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, newX, newY, qDecimals)
    }

    // Same as price_after_constant_product_swap_i_to_q but simulates a swap from Q to I.
    // The max allowed to swap is MAX_POOL_COIN_COUNT. Supply amounts must be > 0.
    // Returns 0 if inputs are invalid.
    fun price_after_constant_product_swap_q_to_i(
        i: FixedPoint64,
        q: FixedPoint64,
        qDelta: FixedPoint64,
        qDecimals: u8,
    ): FixedPoint64 {
        let maxSwap = from_u128(MAX_POOL_COIN_COUNT, 0);
        if (is_zero(i) || is_zero(q) || gt(qDelta, maxSwap)) {
            return zero()
        };
        let (newY, newX) = supply_after_constant_product_swap(q, i, qDelta);
        get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, newX, newY, qDecimals)
    }

    // Gets the supply of X and Y after performing a swap from X to Y.
    fun supply_after_constant_product_swap(
        x: FixedPoint64,
        y: FixedPoint64,
        xDelta: FixedPoint64,
    ): (FixedPoint64, FixedPoint64) {
        let k = multiply_trunc(x, y);
        let newX = add(x, xDelta);
        let newY = divide_trunc(k, newX);
        (newX, newY)
    }

    // Simulates a stable swap and returns the price (X in terms of Y) of the pool after the swap. Uses the following
    // invariant: x - A/x + y - A/y = D, where A is an amplification factor and D is the number of coins in the pool.
    // The larger A is, the more the curve behaves like a constant product curve. The smaller A is, the more the curve
    // behaves like a constant price curve. We set A = 10*D.
    //
    // Takes in and returns fixed points with 10 decimals places.
    fun price_after_stable_swap_x_to_y(x: u128, y: u128, xDelta: u128): u128 {
        // TODO: refactor to use FixedPoint64 since it now supports numbers > MAX_U64.
        let d = x + y;
        let amp = d * 10;
        let newX = x + xDelta;
        // Compute new ySupply by solving for y in the invariant (uses quadratic formula).
        let c = amp;
        let aDivX = amp * FP_EXP / newX;
        let b = d + aDivX - newX;
        let bSqrd = b * b / FP_EXP;
        let sqrtTerm = bSqrd + 4 * c;
        let sqrt = math::sqrt_u128(sqrtTerm);
        let newY = (sqrt + b) / 2;
        // Compute and return new price.
        let x2 = newX * newX / FP_EXP;
        let y2 = newY * newY / FP_EXP;
        let x2y2Ratio = y2 * FP_EXP / x2;
        let sumRatio = (x2 + amp) * FP_EXP / (y2 + amp);
        x2y2Ratio * sumRatio / FP_EXP
    }

    // Same as price_after_stable_swap_x_to_y but simulates a swap from Y to X.
    fun price_after_stable_swap_y_to_x(x: u128, y: u128, yDelta: u128): u128 {
        // TODO: refactor to use FixedPoint64 since it now supports numbers > MAX_U64.
        let d = x + y;
        let amp = d * 10;
        let newY = y + yDelta;
        // Compute new xSupply by solving for x in the invariant (uses quadratic formula).
        let c = amp;
        let aDivY = amp * FP_EXP / newY;
        let b = d + aDivY - newY;
        let bSqrd = b * b / FP_EXP;
        let sqrtTerm = bSqrd + 4 * c;
        let sqrt = math::sqrt_u128(sqrtTerm);
        let newX = (sqrt + b) / 2;
        // Compute and return new price.
        let x2 = newX * newX / FP_EXP;
        let y2 = newY * newY / FP_EXP;
        let x2y2Ratio = y2 * FP_EXP / x2;
        let sumRatio = (x2 + amp) * FP_EXP / (y2 + amp);
        x2y2Ratio * sumRatio / FP_EXP
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
        lpCoinsDecimals: u8,
    ): (FixedPoint64, FixedPoint64, FixedPoint64) {
        let zero = zero();

        assert!(lte(add(xCoinAmt, xSupply), from_u128(MAX_POOL_COIN_COUNT, 0)), ERR_MAX_POOL_COIN_AMT_REACHED);
        assert!(lte(add(yCoinAmt, ySupply), from_u128(MAX_POOL_COIN_COUNT, 0)), ERR_MAX_POOL_COIN_AMT_REACHED);

        if (is_zero(currentLPCoinSupply)) {
            // Can't initialize using a single asset.
            assert!(!is_zero(xCoinAmt) && !is_zero(yCoinAmt), ERR_INIT_WITH_ZERO_ASSET);
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
        } else if (lt(xRatio, yRatio)) {
            (xCoinAmt, multiply_round_up(xRatio, ySupply))
        } else {
            (xCoinAmt, yCoinAmt)
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

        let lpCoinsToMint = trunc_to_decimals(min(yLPCoins, xLPCoins), lpCoinsDecimals);
        assert!(!is_zero(lpCoinsToMint), ERR_DEPOSIT_PRECISION_LOSS);
        assert!(lte(add(lpCoinsToMint, currentLPCoinSupply), from_u128(MAX_LP_COINS, 0)), ERR_MAX_LP_COIN_AMT_REACHED);

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
        assert!(gte(currentLPCoinSupply, lpCoinsToBurn), ERR_INVALID_LP_TOKEN_AMT);
        assert!(!is_zero(xSupply), ERR_INVALID_X_SUPPLY);
        assert!(!is_zero(ySupply), ERR_INVALID_Y_SUPPLY);
        assert!(!is_zero(currentLPCoinSupply), ERR_INVALID_LP_TOKEN_SUPPLY);
        assert!(!is_zero(lpCoinsToBurn), ERR_INVALID_LP_TOKEN_AMT);

        let xTokens = divide_trunc(multiply_trunc(lpCoinsToBurn, xSupply), currentLPCoinSupply);
        let yTokens = divide_trunc(multiply_trunc(lpCoinsToBurn, ySupply), currentLPCoinSupply);

        (trunc_to_decimals(xTokens, xDecimals), trunc_to_decimals(yTokens, yDecimals))
    }

    // Returns the price of I in terms of Q, truncated to the decimal places provided.
    fun get_pool_price(type: u8, iSupply: FixedPoint64, qSupply: FixedPoint64, decimals: u8): FixedPoint64 {
        if (is_zero(iSupply) || is_zero(qSupply)) {
            return zero()
        };

        if (type == POOL_TYPE_CONSTANT_PRODUCT) {
            // Derivative of price curve simplifies to Q/I.
            trunc_to_decimals(divide_trunc(qSupply, iSupply), decimals)
        } else if (type == POOL_TYPE_STABLE_SWAP) {
            // Derivative of price curve simplifies to ((I^2*Q^2)+AQ^2)/((I^2*Q^2)+AI^2).
            // We calculate the above by computing (Q^2)/(I^2) * (I^2 + A)/(Q^2 + A)
            // to avoid overflowing because I^2*Q^2 can be too large of a number.
            let d = add(iSupply, qSupply);
            let amp = multiply_trunc(d, from_u128(10, 0));
            let i2 = multiply_trunc(iSupply, iSupply);
            let q2 = multiply_trunc(qSupply, qSupply);
            let q2i2Ratio = divide_trunc(q2, i2);
            let sumRatio = divide_trunc(add(i2, amp), add(q2, amp));
            trunc_to_decimals(multiply_trunc(q2i2Ratio, sumRatio), decimals)
        } else {
            abort ERR_INVALID_POOL_TYPE
        }
    }

    fun get_order_qty(
        iSupply: FixedPoint64,
        qSupply: FixedPoint64,
        price: FixedPoint64,
        side: u8,
        numberOfOrders: u64,
        iDecimals: u8,
    ): FixedPoint64 {
        let orderCount = from_u64(numberOfOrders, 0);
        if (side == SIDE_SELL) {
            // We want to sell an even proportion of I for each order.
            trunc_to_decimals(divide_trunc(iSupply, orderCount), iDecimals)
        } else {
            // We want to sell an even proportion of Q for each order. The quantity is how much I we can buy at the
            // given price.
            let qToSpend = divide_trunc(qSupply, orderCount);
            trunc_to_decimals(multiply_trunc(qToSpend, price), iDecimals)
        }
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
            zero(), zero(), xAmt, 10, zero(), yAmt, 10, 8);
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
            zero(), zero(), xAmt, 10, zero(), yAmt, 10, 8);
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
            currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10, 8);
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
            currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10, 8);
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
            currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10, 8);
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
            currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10, 8);
        assert!(is_zero(xToReturn), 0);
        assert!(eq(yToReturn, from_u128(3, 10)), 0);
        assert!(
            eq(lpTokens, from_u128(333333333300, 10)),
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

        deposit_multi_asset(currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10, 8);
    }

    #[test]
    fun test_deposit_making_supply_and_lp_token_reach_max() {
        let xSupply = from_u128(MAX_POOL_COIN_COUNT - 1, 0);
        let ySupply = from_u128(MAX_POOL_COIN_COUNT - 1, 0);
        let currentLPTokenSupply = from_u128(MAX_LP_COINS - 1, 0);

        let (lpCoins, unusedX, unusedY) = deposit_multi_asset(
            currentLPTokenSupply,
            xSupply,
            one(),
            10,
            ySupply,
            one(),
            10,
            8,
        );
        assert!(eq(lpCoins, from_u128(1, 0)), 0);
        assert!(eq(unusedX, zero()), 0);
        assert!(eq(unusedY, zero()), 0);
    }

    #[test]
    #[expected_failure(abort_code = 114)]
    fun test_deposit_when_lp_token_is_at_max() {
        let xSupply = from_u128(MAX_POOL_COIN_COUNT - 1, 0);
        let ySupply = from_u128(MAX_POOL_COIN_COUNT - 1, 0);
        let currentLPTokenSupply = from_u128(MAX_LP_COINS, 0);

        deposit_multi_asset(
            currentLPTokenSupply,
            xSupply,
            one(),
            10,
            ySupply,
            one(),
            10,
            8,
        );
    }

    #[test]
    #[expected_failure(abort_code = 113)]
    fun test_deposit_when_adding_more_than_max_to_supply() {
        let xSupply = from_u128(MAX_POOL_COIN_COUNT, 0);
        let ySupply = from_u128(MAX_POOL_COIN_COUNT, 0);
        let currentLPTokenSupply = from_u128(MAX_LP_COINS, 0);

        deposit_multi_asset(
            currentLPTokenSupply,
            xSupply,
            one(),
            10,
            ySupply,
            one(),
            10,
            8,
        );
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
            zero(), zero(), xAmt, 10, zero(), yAmt, 10, 8);
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
            currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10, 8);
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

    #[test]
    fun test_withdraw_multi_asset_pool_with_another_user() {
        // Get some LP tokens.
        let currentLPTokenSupply = from_u128(100, 0);
        let xSupply = from_u128(500, 0);
        let ySupply = from_u128(250, 0);
        let xAmt = from_u128(250, 0);
        let yAmt = from_u128(125, 0);
        let (lpCoinsMinted1, _, _) = deposit_multi_asset(
            currentLPTokenSupply, xSupply, xAmt, 10, ySupply, yAmt, 10, 8);
        assert!(eq(lpCoinsMinted1, from_u128(50, 0)), 0);

        // Simulate another user adding LP tokens.
        let newLPTokenSupply = from_u128(150, 0);
        let newXSupply = from_u128(750, 0);
        let newYSupply = from_u128(375, 0);
        let xAmt2 = from_u128(100, 0);
        let yAmt2 = from_u128(55, 0);
        let (lpCoinsMinted2, unusedX, unusedY) = deposit_multi_asset(
            newLPTokenSupply, newXSupply, xAmt2, 10, newYSupply, yAmt2, 10, 8);
        assert!(eq(lpCoinsMinted2, from_u128(1999999999, 8)), 0);
        assert!(eq(unusedY, from_u128(50000000125, 10)), 0);
        assert!(eq(unusedX, zero()), 0);

        // Swap back minted tokens for pool assets.
        let (xCoinsOut, yCoinsOut) = withdraw_multi_asset(
            lpCoinsMinted1,
            from_u128(169999999995, 9),
            from_u128(850, 0),
            10,
            from_u128(4249999999875, 10),
            10,
        );
        // Note that rounding error contribuutes to this not being exactly 250.
        assert!(eq(xCoinsOut, from_u128(2500000000073, 10)), 0);
        assert!(eq(yCoinsOut, from_u128(125, 0)), 0);
    }

    #[test]
    fun test_withdraw_multi_asset_when_supply_and_lp_token_is_at_max() {
        let xAmt = from_u128(MAX_POOL_COIN_COUNT, 0);
        let yAmt = from_u128(MAX_POOL_COIN_COUNT, 0);
        let xSupply = from_u128(MAX_POOL_COIN_COUNT, 0);
        let ySupply = from_u128(MAX_POOL_COIN_COUNT, 0);
        let currentLPTokenSupply = from_u128(MAX_LP_COINS, 0);

        let (xCoinsOut, yCoinsOut) = withdraw_multi_asset(
            currentLPTokenSupply,
            currentLPTokenSupply,
            xSupply,
            10,
            ySupply,
            10,
        );
        assert!(eq(xCoinsOut, xAmt), 0);
        assert!(eq(yCoinsOut, yAmt), 0);
    }

    //
    // Min Delta Tests.
    //

    #[test]
    fun test_min_delta_i_constant_product() {
        // Current price is 1.
        let x = from_u128(666, 0);
        let y = from_u128(666, 0);
        let target = from_u128(9999, 4);
        let delta = approx_delta_i_for_constant_product_price_change(x, y, target);
        assert!(eq(delta, from_u128(333, 4)), 0);
        let price = price_after_constant_product_swap_i_to_q(x, y, delta, 10);
        assert!(eq(price, from_u128(9999000074, 10)), 0);
    }

    #[test]
    fun test_min_delta_i_constant_product_uneven() {
        // Current price is 0.7507.
        let x = from_u128(666, 0);
        let y = from_u128(500, 0);
        let target = from_u128(7506, 4);
        let delta = approx_delta_i_for_constant_product_price_change(x, y, target);
        assert!(eq(delta, from_u128(6687, 5)), 0);
        let price = price_after_constant_product_swap_i_to_q(x, y, delta, 10);
        assert!(eq(price, from_u128(7506000145, 10)), 0);
    }

    #[test]
    fun test_min_delta_i_constant_product_max() {
        // Current price is 1.
        let x = from_u128(MAX_POOL_COIN_COUNT, 0);
        let y = from_u128(MAX_POOL_COIN_COUNT, 0);
        let target = from_u128(9999, 4);
        let delta = approx_delta_i_for_constant_product_price_change(x, y, target);
        assert!(eq(delta, from_u128(522376382000000, 10)), 0);
        let price = price_after_constant_product_swap_i_to_q(x, y, delta, 10);
        assert!(eq(price, from_u128(9999000000, 10)), 0);
    }

    #[test]
    fun test_min_delta_i_constant_product_max_with_max_decimals() {
        // Current price is 1.
        let minTick = from_u128(1, 5);
        let x = sub(from_u128(MAX_POOL_COIN_COUNT, 0), minTick);
        let y = from_u128(MAX_POOL_COIN_COUNT, 0);
        let target = from_u128(9999, 4);
        let delta = approx_delta_i_for_constant_product_price_change(x, y, target);
        assert!(eq(delta, from_u128(522376382100000, 10)), 0);
        let price = price_after_constant_product_swap_i_to_q(x, y, delta, 10);
        assert!(eq(price, from_u128(9999000000, 10)), 0);
    }

    #[test]
    fun test_min_delta_i_constant_product_min() {
        // Current price is 1.
        let x = from_u128(1, 5);
        let y = from_u128(1, 5);
        let target = from_u128(9999, 4);
        let delta = approx_delta_i_for_constant_product_price_change(x, y, target);
        assert!(is_zero(delta), 0);
    }

    #[test]
    fun test_min_delta_q_constant_product() {
        // Current price is 1.
        let x = from_u128(666, 0);
        let y = from_u128(666, 0);
        let target = from_u128(10001, 4);
        let deltaY = approx_delta_q_for_constant_product_price_change(x, y, target);
        assert!(eq(deltaY, from_u128(333, 4)), 0);
        let price = price_after_constant_product_swap_q_to_i(x, y, deltaY, 10);
        assert!(eq(price, from_u128(10001000025, 10)), 0);
    }

    #[test]
    fun test_min_delta_q_constant_product_uneven() {
        // Current price is 0.7507.
        let x = from_u128(666, 0);
        let y = from_u128(500, 0);
        let target = from_u128(7508, 4);
        let deltaY = approx_delta_q_for_constant_product_price_change(x, y, target);
        assert!(eq(deltaY, from_u128(2185, 5)), 0);
        let price = price_after_constant_product_swap_q_to_i(x, y, deltaY, 10);
        assert!(eq(price, from_u128(7508163678, 10)), 0);
    }

    #[test]
    fun test_min_delta_q_constant_product_max() {
        // Current price is 1.
        let x = from_u128(MAX_POOL_COIN_COUNT, 0);
        let y = from_u128(MAX_POOL_COIN_COUNT, 0);
        let target = from_u128(10001, 4);
        let delta = approx_delta_q_for_constant_product_price_change(x, y, target);
        assert!(eq(delta, from_u128(522298031500000, 10)), 0);
        let price = price_after_constant_product_swap_q_to_i(x, y, delta, 10);
        assert!(eq(price, from_u128(10000999950, 10)), 0);
    }

    #[test]
    fun test_min_delta_q_constant_product_max_with_max_decimals() {
        // Current price is 1.
        let minTick = from_u128(1, 5);
        let x = sub(from_u128(MAX_POOL_COIN_COUNT, 0), minTick);
        let y = from_u128(MAX_POOL_COIN_COUNT, 0);
        let target = from_u128(10001, 4);
        let delta = approx_delta_q_for_constant_product_price_change(x, y, target);
        assert!(eq(delta, from_u128(522298031500000, 10)), 0);
        let price = price_after_constant_product_swap_q_to_i(x, y, delta, 10);
        assert!(eq(price, from_u128(10000999950, 10)), 0);
    }

    #[test]
    fun test_min_delta_q_constant_product_min() {
        // Current price is 1.
        let x = from_u128(1, 5);
        let y = from_u128(1, 5);
        let target = from_u128(10001, 4);
        let delta = approx_delta_q_for_constant_product_price_change(x, y, target);
        assert!(value(delta) == 100000, 0);
        let price = price_after_constant_product_swap_q_to_i(x, y, delta, 10);
        assert!(eq(price, from_u128(40000000000, 10)), 0);
    }

    //
    // Price After Swap Tests.
    //

    #[test]
    fun test_price_after_constant_product_swap_i_to_q() {
        {
            let iSupply = from_u128(500, 0);
            let qSupply = from_u128(500, 0);
            let iCoinsToSwap = from_u128(100, 0);
            let price = price_after_constant_product_swap_i_to_q(iSupply, qSupply, iCoinsToSwap, 10);
            assert!(value(price) == 6944444444, 0);
        };
        {
            // Swapping more than the max.
            let iSupply = from_u128(500, 0);
            let qSupply = from_u128(500, 0);
            let iCoinsToSwap = from_u128(MAX_POOL_COIN_COUNT+1, 0);
            let price = price_after_constant_product_swap_i_to_q(iSupply, qSupply, iCoinsToSwap, 10);
            assert!(is_zero(price), 0);
        };
        {
            // Zero supply.
            let iSupply = zero();
            let qSupply = zero();
            let iCoinsToSwap = from_u128(100, 0);
            let price = price_after_constant_product_swap_i_to_q(iSupply, qSupply, iCoinsToSwap, 10);
            assert!(is_zero(price), 0);
        };
        {
            // When supply is at max value.
            let iSupply = from_u128(MAX_POOL_COIN_COUNT - 100, 0);
            let qSupply = from_u128(MAX_POOL_COIN_COUNT, 0);
            let iCoinsToSwap = from_u128(100, 0);
            let price = price_after_constant_product_swap_i_to_q(iSupply, qSupply, iCoinsToSwap, 10);
            assert!(value(price) == 9999999042, 0);
        };
        {
            // When supply is at min value.
            let iSupply = from_u128(1, 5);
            let qSupply = from_u128(1, 5);
            let iCoinsToSwap = from_u128(1, 5);
            let price = price_after_constant_product_swap_i_to_q(iSupply, qSupply, iCoinsToSwap, 5);
            assert!(eq(price, from_u128(2500000000, 10)), 0);
        };
        {
            // When supply is at min value, and we swap the max coin value.
            let iSupply = from_u128(1, 5);
            let qSupply = from_u128(1, 5);
            let iCoinsToSwap = from_u128(MAX_POOL_COIN_COUNT, 0);
            let price = price_after_constant_product_swap_i_to_q(iSupply, qSupply, iCoinsToSwap, 5);
            assert!(is_zero(price), 0);
        };
        {
            // When supply is at max value with max decimals.
            let minTick = from_u128(1, 5);
            let iSupply = sub(from_u128(MAX_POOL_COIN_COUNT, 0), minTick);
            let qSupply = from_u128(MAX_POOL_COIN_COUNT, 0);
            let iCoinsToSwap = minTick;
            let price = price_after_constant_product_swap_i_to_q(iSupply, qSupply, iCoinsToSwap, 5);
            assert!(value(price) == 9999900000, 0);
        };
        {
            // When supply is at max value, doing 10% of supply.
            let maxCoins = from_u128(MAX_POOL_COIN_COUNT, 0);
            let tenPercent = multiply_trunc(maxCoins, from_u128(1, 1));
            let iSupply = sub(maxCoins, tenPercent);
            let qSupply = from_u128(MAX_POOL_COIN_COUNT, 0);
            let iCoinsToSwap = tenPercent;
            let price = price_after_constant_product_swap_i_to_q(iSupply, qSupply, iCoinsToSwap, 10);
            assert!(value(price) == 9000000000, 0);
        };
        {
            // Uneven initial ratio.
            let iSupply = from_u128(4166666666667, 10);
            let qSupply = from_u128(600, 0);
            let iCoinsToSwap = from_u128(833333333333, 10);
            let price = price_after_constant_product_swap_i_to_q(iSupply, qSupply, iCoinsToSwap, 10);
            assert!(value(price) == 1 * FP_EXP, 0);
        };
        {
            // When supply is at max value, doing 10% of supply.
            let maxCoins = from_u128(MAX_POOL_COIN_COUNT, 0);
            let tenPercent = multiply_trunc(maxCoins, from_u128(1, 1));
            let iSupply = sub(maxCoins, tenPercent);
            let qSupply = from_u128(MAX_POOL_COIN_COUNT, 0);
            let iCoinsToSwap = tenPercent;
            let price = price_after_constant_product_swap_i_to_q(iSupply, qSupply, iCoinsToSwap, 10);
            assert!(value(price) == 9000000000, 0);
        };
    }

    #[test]
    fun test_price_after_constant_product_swap_q_to_i() {
        {
            let iSupply = from_u128(500, 0);
            let qSupply = from_u128(500, 0);
            let qCoinsToSwap = from_u128(100, 0);
            let price = price_after_constant_product_swap_q_to_i(iSupply, qSupply, qCoinsToSwap, 5);
            assert!(eq(price, from_u128(144, 2)), 0);
        };
        {
            // Swapping more than the max.
            let iSupply = from_u128(500, 0);
            let qSupply = from_u128(500, 0);
            let qCoinsToSwap = from_u128(MAX_POOL_COIN_COUNT+1, 0);
            let price = price_after_constant_product_swap_q_to_i(iSupply, qSupply, qCoinsToSwap, 10);
            assert!(is_zero(price), 0);
        };
        {
            // Zero supply.
            let iSupply = zero();
            let qSupply = zero();
            let qCoinsToSwap = from_u128(100, 0);
            let price = price_after_constant_product_swap_i_to_q(iSupply, qSupply, qCoinsToSwap, 10);
            assert!(is_zero(price), 0);
        };
        {
            // When supply is at max value.
            let qSupply = from_u128(MAX_POOL_COIN_COUNT - 100, 0);
            let iSupply = from_u128(MAX_POOL_COIN_COUNT, 0);
            let qCoinsToSwap = from_u128(100, 0);
            let price = price_after_constant_product_swap_q_to_i(iSupply, qSupply, qCoinsToSwap, 5);
            assert!(value(price) == 10000000000, 0);
        };
        {
            // When supply is at min value.
            let iSupply = from_u128(1, 5);
            let qSupply = from_u128(1, 5);
            let qCoinsToSwap = from_u128(1, 5);
            let price = price_after_constant_product_swap_q_to_i(iSupply, qSupply, qCoinsToSwap, 5);
            assert!(eq(price, from_u128(40000000000, 10)), 0);
        };
        {
            // When supply is at min value, and we swap the max coin value.
            let iSupply = from_u128(1, 5);
            let qSupply = from_u128(1, 5);
            let qCoinsToSwap = from_u128(MAX_POOL_COIN_COUNT, 0);
            let price = price_after_constant_product_swap_q_to_i(iSupply, qSupply, qCoinsToSwap, 5);
            assert!(is_zero(price), 0);
        };
        {
            // When supply is at max value with max decimals.
            let minTick = from_u128(1, 5);
            let qSupply = sub(from_u128(MAX_POOL_COIN_COUNT, 0), minTick);
            let iSupply = from_u128(MAX_POOL_COIN_COUNT, 0);
            let qCoinsToSwap = minTick;
            let price = price_after_constant_product_swap_q_to_i(iSupply, qSupply, qCoinsToSwap, 5);
            assert!(value(price) == 10000000000, 0);
        };
        {
            // When supply is at max value, doing 10% of supply.
            let maxCoins = from_u128(MAX_POOL_COIN_COUNT, 0);
            let tenPercent = multiply_trunc(maxCoins, from_u128(1, 1));
            let qSupply = sub(maxCoins, tenPercent);
            let iSupply = from_u128(MAX_POOL_COIN_COUNT, 0);
            let qCoinsToSwap = tenPercent;
            let price = price_after_constant_product_swap_q_to_i(iSupply, qSupply, qCoinsToSwap, 5);
            assert!(value(price) == 11111100000, 0);
        };
        {
            // Uneven initial ratio.
            let qSupply = from_u128(4166666666667, 10);
            let iSupply = from_u128(600, 0);
            let qCoinsToSwap = from_u128(833333333333, 10);
            let price = price_after_constant_product_swap_q_to_i(iSupply, qSupply, qCoinsToSwap, 5);
            assert!(value(price) == 1 * FP_EXP, 0);
        };
    }

    #[test]
    fun test_price_after_stable_swap() {
        {
            let iSupply = 5000000 * FP_EXP;
            let qSupply = 5000000 * FP_EXP;
            let iCoinsToSwap = 100 * FP_EXP;
            let price = price_after_stable_swap_x_to_y(iSupply, qSupply, iCoinsToSwap);
            assert!(price == 9999879996, 0);
        };
        {
            let iSupply = 500 * FP_EXP;
            let qSupply = 500 * FP_EXP;
            let iCoinsToSwap = 100 * FP_EXP;
            let price = price_after_stable_swap_x_to_y(iSupply, qSupply, iCoinsToSwap);
            assert!(price == 8353235099, 0);
        };
        {
            // When supply is at max value.
            let iSupply = MAX_POOL_COIN_COUNT * FP_EXP - 100 * FP_EXP; // Max value - 100.
            let qSupply = MAX_POOL_COIN_COUNT * FP_EXP;
            let iCoinsToSwap = 100 * FP_EXP;
            let price = price_after_stable_swap_x_to_y(iSupply, qSupply, iCoinsToSwap);
            assert!(price == 9999999424, 0);
        };
        {
            // When supply is at max value, doing 10% of supply.
            let iSupply = MAX_POOL_COIN_COUNT * FP_EXP - MAX_POOL_COIN_COUNT * FP_EXP / 10; // Max value * 10%.
            let qSupply = MAX_POOL_COIN_COUNT * FP_EXP;
            let iCoinsToSwap = MAX_POOL_COIN_COUNT * FP_EXP / 10;
            let price = price_after_stable_swap_x_to_y(iSupply, qSupply, iCoinsToSwap);
            assert!(price == 9999999283, 0);
        };
    }

    //
    // Entry Function tests
    //

    #[test(ferum = @ferum, aptos = @0x1)]
    fun test_create_pool(ferum: &signer, aptos: &signer) acquires Pool {
        setup_pool_test(ferum, aptos);

        create_pool_entry<FMA, FMB, ConstantProduct>(ferum);
        let pool = borrow_global<Pool<FMA, FMB, ConstantProduct>>(address_of(ferum));
        assert!(pool.type == POOL_TYPE_CONSTANT_PRODUCT, 0);
        assert!(coin::is_coin_initialized<FerumLP<FMA, FMB, ConstantProduct>>(), 0);

        create_pool_entry<FMA, FMB, StableSwap>(ferum);
        let pool = borrow_global<Pool<FMA, FMB, StableSwap>>(address_of(ferum));
        assert!(pool.type == POOL_TYPE_STABLE_SWAP, 0);
        assert!(coin::is_coin_initialized<FerumLP<FMA, FMB, StableSwap>>(), 0);
    }

    #[test(ferum = @ferum, aptos = @0x1)]
    #[expected_failure(abort_code = 109)]
    fun test_create_duplicate_pool(ferum: &signer, aptos: &signer) {
        setup_pool_test(ferum, aptos);

        create_pool_entry<FMA, FMB, ConstantProduct>(ferum);
        create_pool_entry<FMA, FMB, ConstantProduct>(ferum);
    }

    #[test(ferum = @ferum, aptos = @0x1)]
    #[expected_failure(abort_code = 111)]
    fun test_create_pool_invalid_type(ferum: &signer, aptos: &signer) {
        setup_pool_test(ferum, aptos);

        create_pool_entry<FMA, FMB, FMA>(ferum);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x2)]
    #[expected_failure(abort_code = 108)]
    fun test_create_pool_not_ferum(ferum: &signer, aptos: &signer, user: &signer) {
        setup_pool_test(ferum, aptos);
        account::create_account_for_test(address_of(user));

        create_pool_entry<FMA, FMB, ConstantProduct>(user);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    #[expected_failure(abort_code = 110)]
    fun test_deposit_no_pool(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        account::create_account_for_test(address_of(ferum));
        account::create_account_for_test(address_of(aptos));
        create_fake_coins(ferum, 8);
        register_fma_fmb(ferum, ferum, 50000000000);

        deposit_entry<FMA, FMB, ConstantProduct>(user, 1000000, 1000000);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    fun test_deposit_init_even(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        setup_pool_test(ferum, aptos);
        account::create_account_for_test(address_of(user));
        register_fma_fmb(ferum, user, 50000000000);
        create_pool_entry<FMA, FMB, ConstantProduct>(ferum);

        deposit_entry<FMA, FMB, ConstantProduct>(user, 1000000, 1000000);

        let expectedLPTokens = to_u64(
            from_u128(INITIAL_LP_SUPPLY, 10),
            coin::decimals<FerumLP<FMA, FMB, ConstantProduct>>(),
        );
        assert!(coin::balance<FerumLP<FMA, FMB, ConstantProduct>>(address_of(user)) == expectedLPTokens, 0);
        let pool = borrow_global<Pool<FMA, FMB, ConstantProduct>>(address_of(ferum));
        let poolFMABalance = coin::balance<FMA>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMABalance == 10000000000, 0);
        let poolFMBBalance = coin::balance<FMB>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMBBalance == 10000000000, 0);
        assert!(eq(pool.lpCoinSupply, from_u128(INITIAL_LP_SUPPLY, 10)), 0);
        assert!(eq(pool.iSupply, from_u128(100, 0)), 0);
        assert!(eq(pool.qSupply, from_u128(100, 0)), 0);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    fun test_deposit_init_uneven(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        setup_pool_test(ferum, aptos);
        account::create_account_for_test(address_of(user));
        register_fma_fmb(ferum, user, 50000000000);
        create_pool_entry<FMA, FMB, ConstantProduct>(ferum);

        deposit_entry<FMA, FMB, ConstantProduct>(user, 500000, 1000000);

        let expectedLPTokens = to_u64(
            from_u128(INITIAL_LP_SUPPLY, 10),
            coin::decimals<FerumLP<FMA, FMB, ConstantProduct>>(),
        );
        assert!(coin::balance<FerumLP<FMA, FMB, ConstantProduct>>(address_of(user)) == expectedLPTokens, 0);
        let pool = borrow_global<Pool<FMA, FMB, ConstantProduct>>(address_of(ferum));
        let poolFMABalance = coin::balance<FMA>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMABalance == 5000000000, 0);
        let poolFMBBalance = coin::balance<FMB>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMBBalance == 10000000000, 0);
        assert!(eq(pool.lpCoinSupply, from_u128(INITIAL_LP_SUPPLY, 10)), 0);
        assert!(eq(pool.iSupply, from_u128(50, 0)), 0);
        assert!(eq(pool.qSupply, from_u128(100, 0)), 0);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    fun test_deposit_existing_even(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        setup_pool_test(ferum, aptos);
        account::create_account_for_test(address_of(user));
        register_fma_fmb(ferum, user, 50000000000);
        pool_with_initial_liquidity(ferum, 1000000, 1000000);

        deposit_entry<FMA, FMB, ConstantProduct>(user, 500000, 500000);

        let lpCoinBalance = coin::balance<FerumLP<FMA, FMB, ConstantProduct>>(address_of(user));
        assert!(lpCoinBalance == 5000000000, 0);
        let pool = borrow_global<Pool<FMA, FMB, ConstantProduct>>(address_of(ferum));
        let poolFMABalance = coin::balance<FMA>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMABalance == 15000000000, 0);
        let poolFMBBalance = coin::balance<FMB>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMBBalance == 15000000000, 0);
        assert!(eq(pool.lpCoinSupply, from_u128(150, 0)), 0);
        assert!(eq(pool.iSupply, from_u128(150, 0)), 0);
        assert!(eq(pool.qSupply, from_u128(150, 0)), 0);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    fun test_deposit_existing_uneven(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        // Tests that funds are not withdrawn from user is they are not used to crate LP tokens.

        setup_pool_test(ferum, aptos);
        account::create_account_for_test(address_of(user));
        register_fma_fmb(ferum, user, 50000000000);
        pool_with_initial_liquidity(ferum, 1000000, 1000000);

        deposit_entry<FMA, FMB, ConstantProduct>(user, 500000, 1500000);

        let lpCoinBalance = coin::balance<FerumLP<FMA, FMB, ConstantProduct>>(address_of(user));
        assert!(lpCoinBalance == 5000000000, 0);
        let pool = borrow_global<Pool<FMA, FMB, ConstantProduct>>(address_of(ferum));
        let poolFMABalance = coin::balance<FMA>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMABalance == 15000000000, 0);
        let poolFMBBalance = coin::balance<FMB>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMBBalance == 15000000000, 0);
        assert!(eq(pool.lpCoinSupply, from_u128(150, 0)), 0);
        assert!(eq(pool.iSupply, from_u128(150, 0)), 0);
        assert!(eq(pool.qSupply, from_u128(150, 0)), 0);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    #[expected_failure(abort_code = 113)]
    fun test_deposit_overflow(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        // Tests that we error out when a user tries to put more coins than the pool can support.

        setup_pool_test(ferum, aptos);
        deposit_fma(ferum, ferum, (MAX_POOL_COIN_COUNT as u64));
        account::create_account_for_test(address_of(user));
        register_fma_fmb(ferum, user, (MAX_POOL_COIN_COUNT as u64));
        pool_with_initial_liquidity(ferum, 10446744073709, 1000000);

        deposit_entry<FMA, FMB, ConstantProduct>(user, 500000, 1500000);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    #[expected_failure(abort_code = 110)]
    fun test_withdraw_no_pool(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        account::create_account_for_test(address_of(ferum));
        account::create_account_for_test(address_of(aptos));
        create_fake_coins(ferum, 8);
        register_fma_fmb(ferum, ferum, 50000000000);

        withdraw_entry<FMA, FMB, ConstantProduct>(user, 1000000);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    #[expected_failure(abort_code = 104)]
    fun test_withdraw_no_supply(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        setup_pool_test(ferum, aptos);
        account::create_account_for_test(address_of(user));
        register_fma_fmb(ferum, user, 50000000000);
        create_pool_entry<FMA, FMB, ConstantProduct>(ferum);

        withdraw_entry<FMA, FMB, ConstantProduct>(user, 1000000);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    #[expected_failure]
    fun test_withdraw_no_lp_tokens(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        setup_pool_test(ferum, aptos);
        account::create_account_for_test(address_of(user));
        register_fma_fmb(ferum, user, 50000000000);
        pool_with_initial_liquidity(ferum, 1000000, 1000000);

        withdraw_entry<FMA, FMB, ConstantProduct>(user, 500000);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    fun test_withdraw_all(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        setup_pool_test(ferum, aptos);
        account::create_account_for_test(address_of(user));
        register_fma_fmb(ferum, user, 50000000000);
        pool_with_initial_liquidity(ferum, 1000000, 1000000);

        deposit_entry<FMA, FMB, ConstantProduct>(user, 500000, 500000);
        let lpCoinBalance = coin::balance<FerumLP<FMA, FMB, ConstantProduct>>(address_of(user));
        assert!(lpCoinBalance == 5000000000, 0);
        assert!(coin::balance<FMA>(address_of(user)) == 45000000000, 0);
        assert!(coin::balance<FMB>(address_of(user)) == 45000000000, 0);

        withdraw_entry<FMA, FMB, ConstantProduct>(user, 5000000000);
        let lpCoinBalance = coin::balance<FerumLP<FMA, FMB, ConstantProduct>>(address_of(user));
        assert!(lpCoinBalance == 0, 0);
        let pool = borrow_global<Pool<FMA, FMB, ConstantProduct>>(address_of(ferum));
        let poolFMABalance = coin::balance<FMA>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMABalance == 10000000000, 0);
        let poolFMBBalance = coin::balance<FMB>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMBBalance == 10000000000, 0);
        assert!(eq(pool.lpCoinSupply, from_u128(100, 0)), 0);
        assert!(eq(pool.iSupply, from_u128(100, 0)), 0);
        assert!(eq(pool.qSupply, from_u128(100, 0)), 0);
        assert!(coin::balance<FMA>(address_of(user)) == 50000000000, 0);
        assert!(coin::balance<FMB>(address_of(user)) == 50000000000, 0);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    fun test_withdraw_partial(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        setup_pool_test(ferum, aptos);
        account::create_account_for_test(address_of(user));
        register_fma_fmb(ferum, user, 50000000000);
        pool_with_initial_liquidity(ferum, 1000000, 1000000);

        deposit_entry<FMA, FMB, ConstantProduct>(user, 500000, 500000);
        let lpCoinBalance = coin::balance<FerumLP<FMA, FMB, ConstantProduct>>(address_of(user));
        assert!(lpCoinBalance == 5000000000, 0);
        assert!(coin::balance<FMA>(address_of(user)) == 45000000000, 0);
        assert!(coin::balance<FMB>(address_of(user)) == 45000000000, 0);

        withdraw_entry<FMA, FMB, ConstantProduct>(user, 1000000000);
        let lpCoinBalance = coin::balance<FerumLP<FMA, FMB, ConstantProduct>>(address_of(user));
        assert!(lpCoinBalance == 4000000000, 0);
        let pool = borrow_global<Pool<FMA, FMB, ConstantProduct>>(address_of(ferum));
        let poolFMABalance = coin::balance<FMA>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMABalance == 14000000000, 0);
        let poolFMBBalance = coin::balance<FMB>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMBBalance == 14000000000, 0);
        assert!(eq(pool.lpCoinSupply, from_u128(140, 0)), 0);
        assert!(eq(pool.iSupply, from_u128(140, 0)), 0);
        assert!(eq(pool.qSupply, from_u128(140, 0)), 0);
        assert!(coin::balance<FMA>(address_of(user)) == 46000000000, 0);
        assert!(coin::balance<FMB>(address_of(user)) == 46000000000, 0);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    fun test_withdraw_partial_different_qtys(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        setup_pool_test(ferum, aptos);
        account::create_account_for_test(address_of(user));
        register_fma_fmb(ferum, user, 50000000000);
        pool_with_initial_liquidity(ferum, 1000000, 1600000);

        deposit_entry<FMA, FMB, ConstantProduct>(user, 200000, 320000);
        let lpCoinBalance = coin::balance<FerumLP<FMA, FMB, ConstantProduct>>(address_of(user));
        assert!(lpCoinBalance == 2000000000, 0);
        assert!(coin::balance<FMA>(address_of(user)) == 48000000000, 0);
        assert!(coin::balance<FMB>(address_of(user)) == 46800000000, 0);

        withdraw_entry<FMA, FMB, ConstantProduct>(user, 1500000000);
        let lpCoinBalance = coin::balance<FerumLP<FMA, FMB, ConstantProduct>>(address_of(user));
        assert!(lpCoinBalance == 500000000, 0);
        let pool = borrow_global<Pool<FMA, FMB, ConstantProduct>>(address_of(ferum));
        let poolFMABalance = coin::balance<FMA>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMABalance == 10500000000, 0);
        let poolFMBBalance = coin::balance<FMB>(get_signer_capability_address(&pool.signerCap));
        assert!(poolFMBBalance == 16800000000, 0);
        assert!(eq(pool.lpCoinSupply, from_u128(105, 0)), 0);
        assert!(eq(pool.iSupply, from_u128(105, 0)), 0);
        assert!(eq(pool.qSupply, from_u128(168, 0)), 0);
        assert!(coin::balance<FMA>(address_of(user)) == 49500000000, 0);
        assert!(coin::balance<FMB>(address_of(user)) == 49200000000, 0);
    }

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    fun test_withdraw_when_at_max(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        setup_pool_test(ferum, aptos);
        account::create_account_for_test(address_of(user));
        register_fma_fmb(ferum, user, (MAX_POOL_COIN_COUNT * 100000000 as u64));

        create_pool_entry<FMA, FMB, ConstantProduct>(ferum);
        deposit_entry<FMA, FMB, ConstantProduct>(user, (MAX_POOL_COIN_COUNT * 10000 as u64), 1000000);

        withdraw_entry<FMA, FMB, ConstantProduct>(user, 1500000000);
        let lpCoinBalance = coin::balance<FerumLP<FMA, FMB, ConstantProduct>>(address_of(user));
        assert!(lpCoinBalance == 8500000000, 0);
        let pool = borrow_global<Pool<FMA, FMB, ConstantProduct>>(address_of(ferum));
        assert!(eq(pool.lpCoinSupply, from_u128(85, 0)), 0);
    }

    //
    // Get Pool Price
    //

    #[test]
    fun test_get_pool_price_zero() {
        let i = zero();
        let q = zero();

        let price = get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, i, q, 4);
        assert!(is_zero(price), 0);
        let price = get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, i, q, 0);
        assert!(is_zero(price), 0);
        let price = get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, i, q, 10);
        assert!(is_zero(price), 0);

        let price = get_pool_price(POOL_TYPE_STABLE_SWAP, i, q, 4);
        assert!(is_zero(price), 0);
        let price = get_pool_price(POOL_TYPE_STABLE_SWAP, i, q, 0);
        assert!(is_zero(price), 0);
        let price = get_pool_price(POOL_TYPE_STABLE_SWAP, i, q, 10);
        assert!(is_zero(price), 0);
    }

    #[test]
    fun test_get_pool_price_constant_product_max() {
        let i = from_u128(MAX_POOL_COIN_COUNT, 0);
        let q = from_u128(MAX_POOL_COIN_COUNT, 0);

        let price = get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, i, q, 4);
        assert!(eq(price, from_u128(1, 0)), 0);

        let price = get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, i, q, 0);
        assert!(eq(price, from_u128(1, 0)), 0);

        let price = get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, i, q, 10);
        assert!(eq(price, from_u128(1, 0)), 0);
    }

    #[test]
    fun test_get_pool_price_constant_product_min() {
        let decimals = 4;
        let i = from_u128(1, decimals);
        let q = from_u128(1, decimals);
        let price = get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, i, q, decimals);
        assert!(eq(price, from_u128(1, 0)), 0);

        let decimals = 0;
        let i = from_u128(1, decimals);
        let q = from_u128(1, decimals);
        let price = get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, i, q, decimals);
        assert!(eq(price, from_u128(1, 0)), 0);

        let i = from_u128(1, 5);
        let q = from_u128(1, 5);
        let price = get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, i, q, 5);
        assert!(eq(price, from_u128(1, 0)), 0);
    }

    #[test]
    fun test_get_pool_price_constant_product_max_with_max_decimals() {
        let i = sub(from_u128(MAX_POOL_COIN_COUNT, 0), from_u128(1, 5));
        let q = sub(from_u128(MAX_POOL_COIN_COUNT, 0), from_u128(1, 5));

        let price = get_pool_price(POOL_TYPE_CONSTANT_PRODUCT, i, q, 5);
        assert!(eq(price, from_u128(1, 0)), 0);
    }

    #[test]
    fun test_get_pool_price_stable_swap_max() {
        let i = from_u128(MAX_POOL_COIN_COUNT, 0);
        let q = from_u128(MAX_POOL_COIN_COUNT, 0);

        let price = get_pool_price(POOL_TYPE_STABLE_SWAP, i, q, 4);
        assert!(eq(price, from_u128(1, 0)), 0);

        let price = get_pool_price(POOL_TYPE_STABLE_SWAP, i, q, 0);
        assert!(eq(price, from_u128(1, 0)), 0);

        let price = get_pool_price(POOL_TYPE_STABLE_SWAP, i, q, 10);
        assert!(eq(price, from_u128(1, 0)), 0);
    }

    #[test]
    fun test_get_pool_price_stable_swap_min() {
        let decimals = 4;
        let i = from_u128(1, decimals);
        let q = from_u128(1, decimals);
        let price = get_pool_price(POOL_TYPE_STABLE_SWAP, i, q, decimals);
        assert!(eq(price, from_u128(1, 0)), 0);

        let decimals = 0;
        let i = from_u128(1, decimals);
        let q = from_u128(1, decimals);
        let price = get_pool_price(POOL_TYPE_STABLE_SWAP, i, q, decimals);
        assert!(eq(price, from_u128(1, 0)), 0);

        let i = from_u128(1, 5);
        let q = from_u128(1, 5);
        let price = get_pool_price(POOL_TYPE_STABLE_SWAP, i, q, 5);
        assert!(eq(price, from_u128(1, 0)), 0);
    }

    #[test]
    fun test_get_pool_price_stale_swap_max_with_max_decimals() {
        let i = sub(from_u128(MAX_POOL_COIN_COUNT, 0), from_u128(1, 5));
        let q = sub(from_u128(MAX_POOL_COIN_COUNT, 0), from_u128(1, 5));

        let price = get_pool_price(POOL_TYPE_STABLE_SWAP, i, q, 5);
        assert!(eq(price, from_u128(1, 0)), 0);
    }

    //
    // Price Curve Tests.
    //

    #[test]
    fun test_constant_product_rebalance_price_points() {
        let iSupply = from_u128(100, 0);
        let qSupply = from_u128(100, 0);
        let prices = get_constant_product_rebalance_prices(iSupply, qSupply, 3);
        assert_price_curve(3, prices, vector<u128>[
            1012,
            1010,
            1008,
            1006,
            1004,
            1002,
             998,
             996,
             994,
             992,
             990,
             988,
        ]);
    }

    #[test]
    fun test_constant_product_rebalance_price_points_uneven() {
        let iSupply = from_u128(142441697545, 5);
        let qSupply = from_u128(468210945, 1);
        let prices = get_constant_product_rebalance_prices(iSupply, qSupply, 3);
        assert_price_curve(3, prices, vector<u128>[
            33265,
            33199,
            33133,
            33067,
            33001,
            32936,
            32804,
            32739,
            32674,
            32608,
            32544,
            32479,
        ]);
    }

    #[test]
    fun test_constant_product_rebalance_price_points_max() {
        let iSupply = from_u128(MAX_POOL_COIN_COUNT, 0);
        let qSupply = from_u128(MAX_POOL_COIN_COUNT, 0);
        let prices = get_constant_product_rebalance_prices(iSupply, qSupply, 3);
        assert_price_curve(3, prices, vector<u128>[
            1012,
            1010,
            1008,
            1006,
            1004,
            1002,
            998,
            996,
            994,
            992,
            990,
            988,
        ]);
    }

    #[test]
    fun test_constant_product_rebalance_price_points_min() {
        let iSupply = from_u128(1, 5);
        let qSupply = from_u128(1, 5);
        let prices = get_constant_product_rebalance_prices(iSupply, qSupply, 3);
        assert_price_curve(3, prices, vector<u128>[
            1012,
            1010,
            1008,
            1006,
            1004,
            1002,
            998,
            996,
            994,
            992,
            990,
            988,
        ]);
    }

    //
    // Rebalance Tests.
    //

    #[test(ferum = @ferum, aptos = @0x1, user = @0x3)]
    fun test_rebalance_constant_product(ferum: &signer, aptos: &signer, user: &signer) acquires Pool {
        setup_pool_test(ferum, aptos);
        account::create_account_for_test(address_of(user));
        register_fma_fmb(ferum, user, 1000000000);
        create_pool_entry<FMA, FMB, ConstantProduct>(ferum);

        // Initial pool state and rebalance.
        deposit_entry<FMA, FMB, ConstantProduct>(ferum, 1000000, 900000);
        rebalance_entry<FMA, FMB, ConstantProduct>(user);
        assert_orders_placed_correctly(address_of(ferum));

        // Update the pool supply and rebalance again.
        deposit_entry<FMA, FMB, ConstantProduct>(ferum, 20000, 10000);
        rebalance_entry<FMA, FMB, ConstantProduct>(user);
        assert_orders_placed_correctly(address_of(ferum));
    }

    #[test]
    fun test_divide_bounds() {
        // Tests that the max bounds can be divided by the min bounds without errors.
        let min = from_u128(1, 5);
        let maxCoins = from_u128(MAX_POOL_COIN_COUNT, 0);
        divide_trunc(maxCoins, min);
        let maxLP = from_u128(MAX_LP_COINS, 0);
        divide_trunc(maxLP, min);
    }

    #[test_only]
    fun assert_orders_placed_correctly(ferum: address) acquires Pool {
        let pool = borrow_global<Pool<FMA, FMB, ConstantProduct>>(ferum);
        let poolSigner = &create_signer_with_capability(&pool.signerCap);
        let orderInfos = market::get_order_metadatas_for_owner_external<FMA, FMB>(poolSigner);
        assert!(vector::length(&orderInfos) == 12, 0);
        let (fmaDecimals, fmbDecimals) = market::get_market_decimals<FMA, FMB>();

        let prices = get_constant_product_rebalance_prices(pool.iSupply, pool.qSupply, fmbDecimals);

        let totalOrders = vector::length(&orderInfos);
        let i = totalOrders;
        while (i > 0) {
            let metadata = vector::pop_back(&mut orderInfos);
            let price = get_order_price(&metadata);
            let originalQty = get_order_original_qty(&metadata);
            let side = get_order_side(&metadata);

            let (found, idx) = vector::index_of(&prices, &price);
            assert!(found, 0);
            vector::remove(&mut prices, idx);

            let expectedQty = get_order_qty(pool.iSupply, pool.qSupply, price, side, totalOrders, fmaDecimals);
            assert!(eq(originalQty, expectedQty), 0);

            i = i - 1;
        };
    }

    #[test_only]
    fun assert_price_curve(decimals: u8, actual: vector<FixedPoint64>, expected: vector<u128>) {
        let expectedFP = prices_to_fixed_points(expected, decimals);
        assert!(vectors_eq(&actual, &expectedFP), 0);
    }

    #[test_only]
    fun prices_to_fixed_points(prices: vector<u128>, decimals: u8): vector<FixedPoint64> {
        let out = vector::empty<FixedPoint64>();
        let i = 0;
        let length = vector::length(&prices);
        while (i < length) {
            let price = vector::pop_back(&mut prices);
            vector::push_back(&mut out, from_u128(price, decimals));
            i = i + 1;
        };
        vector::reverse(&mut out);
        out
    }

    #[test_only]
    fun setup_pool_test(ferum: &signer, aptos: &signer) {
        account::create_account_for_test(address_of(ferum));
        account::create_account_for_test(address_of(aptos));
        create_fake_coins(ferum, 8);
        register_fma_fmb(ferum, ferum, 50000000000);
        market::setup_market_for_test<FMA, FMB>(ferum, aptos);
    }

    #[test_only]
    fun pool_with_initial_liquidity(ferum: &signer, coinIAmt: u64, coinQAmt: u64) acquires Pool {
        create_pool_entry<FMA, FMB, ConstantProduct>(ferum);
        deposit_entry<FMA, FMB, ConstantProduct>(ferum, coinIAmt, coinQAmt);
    }

    #[test_only]
    fun vectors_eq<T>(a: &vector<T>, b: &vector<T>): bool {
        let aLength = vector::length(a);
        let bLength = vector::length(b);
        if (aLength != bLength) {
            return false
        };

        let i = 0;
        while (i < aLength) {
            let aVal = vector::borrow(a, i);
            let bVal = vector::borrow(b, i);
            if (aVal != bVal) {
                return false
            };
            i = i + 1;
        };
        true
    }
}
