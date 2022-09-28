module ferum::coin_utils {
    use aptos_std::comparator::{Self, Result};
    use aptos_std::type_info;
    use aptos_framework::coin::{Self};
    use std::string;
    use std::string::String;
    use ferum_std::math;
    #[test_only]
    use ferum::coin_test_helpers::{FMA, FMB, create_fake_coins};
    #[test_only]
    struct NotCoin {}

    // Errors

    /// The provided type is not a valid coin!
    const ERR_IS_NOT_COIN: u64 = 700;
    /// Cannot have a liquidity pool consisting of the same coin!
    const ERR_CANNOT_HAVE_A_PAIR_OF_THE_SAME_COIN: u64 = 701;
    /// When coins used to create pair have wrong ordering.
    const ERR_WRONG_PAIR_ORDERING: u64 = 702;

    // Public Functions

    public fun assert_valid_sorted_coin_pair<X, Y>() {
        // 1. The provided coins must be initialized!
        assert!(coin::is_coin_initialized<X>(), ERR_IS_NOT_COIN);
        assert!(coin::is_coin_initialized<Y>(), ERR_IS_NOT_COIN);
        let coinPairOrder = compare<X, Y>();
        // 2. Cannot provide a pair of the same coin!
        assert!(!comparator::is_equal(&coinPairOrder), ERR_CANNOT_HAVE_A_PAIR_OF_THE_SAME_COIN);
        // 3. Assert the correct order of coins! X != Y && X.symbol < Y.symbol
        assert!(comparator::is_smaller_than(&coinPairOrder), ERR_WRONG_PAIR_ORDERING);
    }

    public fun get_coin_pair_name_and_symbol<X, Y>(): (String, String) {
        let coinName = string::utf8(b"");
        string::append_utf8(&mut coinName, b"LP-");
        string::append(&mut coinName, coin::symbol<X>());
        string::append_utf8(&mut coinName, b"-");
        string::append(&mut coinName, coin::symbol<Y>());

        let coinSymbol = string::utf8(b"");
        string::append(&mut coinSymbol, coin_symbol_prefix<X>());
        string::append_utf8(&mut coinSymbol, b"-");
        string::append(&mut coinSymbol, coin_symbol_prefix<Y>());

        (coinName, coinSymbol)
    }

    fun coin_symbol_prefix<CoinType>(): String {
        let symbol = coin::symbol<CoinType>();
        // TODO: Ideally, we can extract out a more legible name here.
        let prefix_length = math::min_u64(string::length(&symbol), 2);
        string::sub_string(&symbol, 0, prefix_length)
    }

    /// Check that coins generics `X`, `Y` are sorted in correct ordering.
    /// X != Y && X.symbol < Y.symbol
    public fun is_sorted<X, Y>(): bool {
        let order = compare<X, Y>();
        assert!(!comparator::is_equal(&order), ERR_CANNOT_HAVE_A_PAIR_OF_THE_SAME_COIN);
        comparator::is_smaller_than(&order)
    }

    // Private Helpers

    /// Compare two coins, `X` and `Y`, using names.
    /// Caller should call this function to determine the order of A, B.
    fun compare<X, Y>(): Result {
        let xTypeInfo = type_info::type_of<X>();
        let yTypeInfo = type_info::type_of<Y>();

        // 1. Compare struct_name.
        let xStructName = type_info::struct_name(&xTypeInfo);
        let yStructName = type_info::struct_name(&yTypeInfo);
        let structNameComparator = comparator::compare(&xStructName, &yStructName);
        if (!comparator::is_equal(&structNameComparator)) return structNameComparator;

        // 2. If struct names are equal, compare module name.
        let xModuleName = type_info::module_name(&xTypeInfo);
        let yModuleName = type_info::module_name(&yTypeInfo);
        let moduleNameComparator = comparator::compare(&xModuleName, &yModuleName);
        if (!comparator::is_equal(&moduleNameComparator)) return moduleNameComparator;

        // 3. If modules are equal, compare addresses.
        let yAddress = type_info::account_address(&xTypeInfo);
        let bAddress = type_info::account_address(&yTypeInfo);
        let addressComparator = comparator::compare(&yAddress, &bAddress);

        addressComparator
    }

    // Tests

    #[test]
    #[expected_failure(abort_code = 700)]
    fun test_assert_valid_coin_pair_with_invalid_coin_type() {
        assert_valid_sorted_coin_pair<NotCoin, FMB>();
    }

    #[test(signer= @ferum)]
    #[expected_failure(abort_code = 701)]
    fun test_assert_valid_coin_pair_with_pair_of_same_coin_types(signer: &signer) {
        create_fake_coins(signer, 10);
        assert_valid_sorted_coin_pair<FMA, FMA>();
    }

    #[test(signer= @ferum)]
    #[expected_failure(abort_code = 702)]
    fun test_assert_valid_coin_pair_with_invalid_pair_order(signer: &signer) {
        create_fake_coins(signer, 10);
        assert_valid_sorted_coin_pair<FMB, FMA>();
    }

    #[test(signer= @ferum)]
    fun test_assert_valid_coin_pair_with_valid_coins(signer: &signer) {
        create_fake_coins(signer, 10);
        assert_valid_sorted_coin_pair<FMA, FMB>();
    }
}