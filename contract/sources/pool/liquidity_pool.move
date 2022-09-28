
module ferum::liquidity_pool {
    use ferum_std::math::{sqrt_u128};
    use aptos_framework::coin::{Self, Coin};
    use ferum::lp_coin::{LPCoin};
    use ferum::coin_utils::{get_coin_pair_name_and_symbol};
    use std::signer::address_of;
    use std::option;
    use ferum::coin_utils::{assert_valid_sorted_coin_pair};
    #[test_only]
    use ferum::coin_test_helpers::{FMA, FMB, create_fake_coins, register_fma, register_fmb};
    #[test_only]
    use aptos_framework::account;
    use ferum_std::fixed_point_64::{from_u128, multiply_trunc, divide_trunc, divide_round_up, from_u64, to_u64_round_up, to_u64_trunc, value, multiply_round_up, new_u128};
    use ferum_std::fixed_point_64::FixedPoint64;
    use ferum_std::fixed_point_64;
    #[test_only]
    use aptos_std::debug::print;

    // Constants

    /// Minimum liquidity is required to be 1000 times the minimum quantity of pool shares.
    /// If LP Coin has 8 decimal points, then the minimum is 0.00000001
    const MINIMUM_LIQUIDITY: u128 = 1000;

    // Errors.

    /// Attempting to use a pool before initializing it.
    const ERR_POOL_NOT_INITIALIZED: u64 = 100;
    /// Attempting to initialize an already existing pool!
    const ERR_POOL_ALREADY_INITIALIZED: u64 = 101;
    /// Attempting to call a function that requires admin permissions without a admin permission.
    const ERR_REQUIRES_ADMIN_PERMISSION: u64 = 200;
    /// Attempting to provide liquidity less then the required minimimum liquidity amount.
    const ERR_NOT_ENOUGH_LIQUIDITY: u64 = 300;
    const ERR_INVALID_LP_BURN: u64 = 400;
    /// At least one of the input coins needs to be greater than zero.
    const ERR_INVALID_SWAP_INPUT: u64 = 500;
    /// When during swap, ab=k math doesn't work out!
    const ERR_INVALID_SWAP_RATIO: u64 = 501;

    // Structs

    /// Stores the entire state of the pool between pairs X & Y.
    struct LiquidityPool<phantom  X, phantom Y> has key {
        xCoinReserve: Coin<X>,
        yCoinReserve: Coin<Y>,
        lpCoinMintCapability: coin::MintCapability<LPCoin<X, Y>>,
        lpCoinBurnCapability: coin::BurnCapability<LPCoin<X, Y>>,
    }

    public fun init_pool<X, Y>(poolAdmin: &signer) {
        assert!(address_of(poolAdmin) == @ferum, ERR_REQUIRES_ADMIN_PERMISSION);
        assert!(!exists<LiquidityPool<X, Y>>(@ferum), ERR_POOL_ALREADY_INITIALIZED);
        assert_valid_sorted_coin_pair<X, Y>();
        let (lpCoinName, lpCoinSymbol) = get_coin_pair_name_and_symbol<X, Y>();
        let (lpCoinBurnCap,
            lpCoinFreezeCap,
            lpCoinMintCap) =
            coin::initialize<LPCoin<X, Y>>(
                poolAdmin,
                lpCoinName,
                lpCoinSymbol,
                8,
                true
            );
        coin::destroy_freeze_cap(lpCoinFreezeCap);
        let pool = LiquidityPool<X, Y> {
            xCoinReserve: coin::zero<X>(),
            yCoinReserve: coin::zero<Y>(),
            lpCoinBurnCapability: lpCoinBurnCap,
            lpCoinMintCapability: lpCoinMintCap,
        };
        move_to(poolAdmin, pool);
    }

    public fun pool_exists<X, Y>(): bool {
        exists<LiquidityPool<X, Y>>(@ferum)
    }

    /// Returns price of X in terms of Y i.e. p = xReserve / yReserve.
    public fun x_price<X, Y>(): FixedPoint64 acquires  LiquidityPool {
        assert!(exists<LiquidityPool<X, Y>>(@ferum), ERR_POOL_NOT_INITIALIZED);
        let pool = borrow_global_mut<LiquidityPool<X, Y>>(@ferum);
        let xReserveFP = from_u64(coin::value(&pool.xCoinReserve), coin::decimals<X>());
        let yReserveFP = from_u64(coin::value(&pool.yCoinReserve), coin::decimals<Y>());
        divide_round_up(yReserveFP, xReserveFP)
    }

    /// Returns price of X in terms of Y i.e. p = yReserve / xReserve.
    public fun y_price<X, Y>(): FixedPoint64 acquires LiquidityPool {
        assert!(exists<LiquidityPool<X, Y>>(@ferum), ERR_POOL_NOT_INITIALIZED);
        let pool = borrow_global_mut<LiquidityPool<X, Y>>(@ferum);
        let xReserveFP = from_u64(coin::value(&pool.xCoinReserve), coin::decimals<X>());
        let yReserveFP = from_u64(coin::value(&pool.yCoinReserve), coin::decimals<Y>());
        divide_round_up(xReserveFP, yReserveFP)
    }

    /// For providing additional liquidity to the AMM, the signer mints additional LP coins.
    public fun mint<X, Y>(coinX: Coin<X>, coinY: Coin<Y>): Coin<LPCoin<X, Y>> acquires LiquidityPool {
        assert!(exists<LiquidityPool<X, Y>>(@ferum), ERR_POOL_NOT_INITIALIZED);
        assert_valid_sorted_coin_pair<X, Y>();

        let pool = borrow_global_mut<LiquidityPool<X, Y>>(@ferum);

        let xCoinProvided = coin_fp_value(&coinX);
        let yCoinProvided = coin_fp_value(&coinY);
        let xCoinReserve = coin_fp_value(&pool.xCoinReserve);
        let yCoinReserve = coin_fp_value(&pool.yCoinReserve);
        let lpSupply = coin_fp_supply<LPCoin<X, Y>>();

        let lpProvided = if (fixed_point_64::value(lpSupply) == 0) {
            // If this is the initial deposit, LP's equal to the geometric mean of the pair.
            let initialLiqudity = sqrt_u128(value(xCoinProvided) * value(yCoinProvided));
            print(&fixed_point_64::new_u128(initialLiqudity));
            assert!(initialLiqudity > MINIMUM_LIQUIDITY, ERR_NOT_ENOUGH_LIQUIDITY);
            initialLiqudity - MINIMUM_LIQUIDITY
        } else {
            // If not, choose the least amount of LP added to ensure there is incentive to preserve the ratio.
            fixed_point_64::value(
                fixed_point_64::min(divide_trunc(multiply_trunc(xCoinProvided, lpSupply), xCoinReserve),
                    divide_trunc(multiply_trunc(yCoinProvided, lpSupply), yCoinReserve)))
        };
        assert!(lpProvided > MINIMUM_LIQUIDITY, ERR_NOT_ENOUGH_LIQUIDITY);

        coin::merge(&mut pool.xCoinReserve, coinX);
        coin::merge(&mut pool.yCoinReserve, coinY);

        coin::mint<LPCoin<X, Y>>(to_u64_trunc(new_u128(lpProvided), coin::decimals<LPCoin<X, Y>>()), &pool.lpCoinMintCapability)
    }

    fun lp_from_provided_coin(provided: u64, reserve: u64, lpSupply: u128) : u64 {
        let provided_u128 = (provided as u128);
        let reserve_u128 = (reserve as u128);
        (((provided_u128 * lpSupply) / reserve_u128) as u64)
    }

    fun coin_fp_value<c>(coin: &Coin<c>): FixedPoint64 {
        from_u64(coin::value(coin), coin::decimals<c>())
    }

    fun coin_fp_extract_trunc<c>(coin: &mut Coin<c>, amount: FixedPoint64): Coin<c> {
        coin::extract(coin, to_u64_trunc(amount, coin::decimals<c>()))
    }

    public fun burn<X, Y>(lpCoins: Coin<LPCoin<X, Y>>): (Coin<X>, Coin<Y>) acquires LiquidityPool {
        assert!(exists<LiquidityPool<X, Y>>(@ferum), ERR_POOL_NOT_INITIALIZED);
        assert_valid_sorted_coin_pair<X, Y>();

        let pool = borrow_global_mut<LiquidityPool<X, Y>>(@ferum);

        let xCoinReserve = coin_fp_value(&pool.xCoinReserve);
        let yCoinReserve = coin_fp_value(&pool.yCoinReserve);
        let lpCoinSupply = coin_fp_supply<LPCoin<X, Y>>();
        let lpCoinsBurnt = coin_fp_value(&lpCoins);

        let xCoinToExtract = divide_trunc(multiply_trunc(lpCoinsBurnt, xCoinReserve), lpCoinSupply);
        let yCoinToExtract = divide_trunc(multiply_trunc(lpCoinsBurnt, yCoinReserve), lpCoinSupply);
        assert!(value(xCoinToExtract) > 0 && value(yCoinToExtract) > 0, ERR_INVALID_LP_BURN);

        let xCoinExtracted = coin_fp_extract_trunc(&mut pool.xCoinReserve, xCoinToExtract);
        let yCoinExtracted = coin_fp_extract_trunc(&mut pool.yCoinReserve, yCoinToExtract);

        coin::burn(lpCoins, &pool.lpCoinBurnCapability);

        (xCoinExtracted, yCoinExtracted)
    }

    public fun swap_pair<X, Y>(xCoinIn: Coin<X>, xCoinOut: u64, yCoinIn: Coin<Y>, yCoinOut: u64): (Coin<X>, Coin<Y>) acquires LiquidityPool {
        assert!(exists<LiquidityPool<X, Y>>(@ferum), ERR_POOL_NOT_INITIALIZED);
        assert_valid_sorted_coin_pair<X, Y>();

        let pool = borrow_global_mut<LiquidityPool<X, Y>>(@ferum);

        let xCoinInValue = coin::value(&xCoinIn);
        let yCoinInInvalue = coin::value(&yCoinIn);
        assert!(xCoinInValue > 0 || yCoinInInvalue > 0, ERR_INVALID_SWAP_INPUT);

        let xCoinReserveInitial = coin_fp_value(&pool.xCoinReserve);
        let yCoinReserveInitial = coin_fp_value(&pool.yCoinReserve);

        coin::merge(&mut pool.xCoinReserve, xCoinIn);
        coin::merge(&mut pool.yCoinReserve, yCoinIn);

        let xCoinOutExtract = coin::extract(&mut pool.xCoinReserve, xCoinOut);
        let yCoinOutExtract = coin::extract(&mut pool.yCoinReserve, yCoinOut);

        let xCoinReserveFinal = coin_fp_value(&pool.xCoinReserve);
        let yCoinReserveFinal = coin_fp_value(&pool.yCoinReserve);

        let kInitial = multiply_round_up(xCoinReserveInitial, yCoinReserveInitial);
        let kFinal = multiply_round_up(xCoinReserveFinal, yCoinReserveFinal);

        assert!(fixed_point_64::gte( kFinal, kInitial), ERR_INVALID_SWAP_RATIO);

        (xCoinOutExtract, yCoinOutExtract)
    }

    fun coin_fp_supply<c>(): FixedPoint64 {
        from_u128(option::extract(&mut coin::supply<c>()), coin::decimals<c>())
    }

    #[test(poolAdmin = @ferum)]
    fun test_init_pool(poolAdmin: &signer) {
        account::create_account_for_test(address_of(poolAdmin));
        create_fake_coins(poolAdmin, 10);
        init_pool<FMA, FMB>(poolAdmin);
        assert!(exists<LiquidityPool<FMA, FMB>>(@ferum), 0);
    }

    #[test(poolAdmin = @ferum, user = @0xCAFE)]
    fun test_mint(poolAdmin: &signer, user: &signer) acquires LiquidityPool {
        // Testing secondary provisionin of liquidity; dLP = min( dX / x * lp, dY / y * lp);
        account::create_account_for_test(address_of(poolAdmin));
        account::create_account_for_test(address_of(user));
        create_fake_coins(poolAdmin, 3);
        register_fma(poolAdmin, user, 20000); // 20 FMA
        register_fmb(poolAdmin, user, 30000); // 30 FMB
        coin::register<LPCoin<FMA, FMB>>(user);

        init_pool<FMA, FMB>(poolAdmin);

        // LP = sqrt(A * B) - 1000
        let coinA = coin::withdraw<FMA>(user, 10000);
        let coinB = coin::withdraw<FMB>(user, 20000);
        let lp = mint<FMA, FMB>(coinA, coinB);
        assert!(to_u64_round_up(x_price<FMA, FMB>(), coin::decimals<FMB>()) == 2000, 0);
        assert!(to_u64_round_up(y_price<FMA, FMB>(), coin::decimals<FMA>()) == 500, 0);
        print(&to_u64_trunc(coin_fp_supply<LPCoin<FMA, FMB>>(), 3));
        //assert!(get_lp_coin_supply<FMA, FMB>() == 13142, 0);
        coin::deposit(address_of(user), lp);

        // LP = min( dX / x * lp,  dY / y * lp);
        let coinA = coin::withdraw<FMA>(user, 10000);
        let coinB = coin::withdraw<FMB>(user, 10000);
        let lp = mint<FMA, FMB>(coinA, coinB);
        assert!(to_u64_round_up(x_price<FMA, FMB>(), coin::decimals<FMB>()) == 1500, 0);
        assert!(to_u64_round_up(y_price<FMA, FMB>(), coin::decimals<FMA>()) == 667, 0);
        //assert!(get_lp_coin_supply<FMA, FMB>() == 19713, 0);
        //assert!(coin::value(&lp) == 6571, 0);
        coin::deposit(address_of(user), lp);
    }

    #[test(poolAdmin = @ferum, user = @0xCAFE)]
    fun test_burn(poolAdmin: &signer, user: &signer) acquires LiquidityPool {
        // Testing converting LP coins back to coin A, B.
        account::create_account_for_test(address_of(poolAdmin));
        account::create_account_for_test(address_of(user));
        create_fake_coins(poolAdmin, 3);
        register_fma(poolAdmin, user, 20000); // 20 FMA
        register_fmb(poolAdmin, user, 30000); // 30 FMB
        coin::register<LPCoin<FMA, FMB>>(user);

        init_pool<FMA, FMB>(poolAdmin);

        // LP = sqrt(A * B) - 1000
        let coinA = coin::withdraw<FMA>(user, 10000);
        let coinB = coin::withdraw<FMB>(user, 20000);
        let lp = mint<FMA, FMB>(coinA, coinB);


        let burnLP = coin::extract(&mut lp, 1000);

        let (coinA, coinB) = burn<FMA, FMB>(burnLP);
        assert!(coin::value(&coinA) == 760, 0);
        assert!(coin::value(&coinB) == 1521, 0);

        coin::deposit(address_of(user), lp);
        coin::deposit(address_of(user), coinA);
        coin::deposit(address_of(user), coinB);
    }

    #[test(poolAdmin = @ferum, user = @0xCAFE)]
    fun test_swap_pair(poolAdmin: &signer, user: &signer) acquires LiquidityPool {
        // Testing swap two coins.
        account::create_account_for_test(address_of(poolAdmin));
        account::create_account_for_test(address_of(user));
        create_fake_coins(poolAdmin, 3);
        register_fma(poolAdmin, user, 20000); // 20 FMA
        register_fmb(poolAdmin, user, 30000); // 30 FMB
        coin::register<LPCoin<FMA, FMB>>(user);

        init_pool<FMA, FMB>(poolAdmin);

        // LP = sqrt(A * B) - 1000
        let coinA = coin::withdraw<FMA>(user, 10000);
        let coinB = coin::withdraw<FMB>(user, 20000);
        let lp = mint<FMA, FMB>(coinA, coinB);
        //assert!(get_lp_coin_supply<FMA, FMB>() == 13142, 0);
        coin::deposit(address_of(user), lp);

        // Swap 1000 coin A for 1818 coin B.
        // Initial k = 10000 * 20000 = 200000000
        // Final k = (10000 + 1000) * (20000 - 1818) = 200002000
        // Final k > Initial K
        let coinA = coin::withdraw<FMA>(user, 1000);
        let (coinA, coinB) = swap_pair<FMA, FMB>(coinA, 0, coin::zero<FMB>(), 1818);

        assert!(coin::value(&coinA) == 0, 0);
        assert!(coin::value(&coinB) == 1818, 0);

        coin::deposit(address_of(user), coinA);
        coin::deposit(address_of(user), coinB);
    }
}
