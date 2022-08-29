module ferum::ferum {
    use aptos_std::table;
    use std::signer::address_of;
    use std::string;
    use aptos_std::type_info;
    #[test_only]
    use ferum::coin_test_helpers;
    #[test_only]
    use aptos_framework::account;

    ///
    /// Errors
    ///

    const ERR_NOT_ALLOWED: u64 = 0;
    const ERR_MARKET_NOT_EXISTS: u64 = 1;
    const ERR_MARKET_EXISTS: u64 = 2;

    ///
    /// Structs.
    ///

    /// Global info object for ferum.
    struct FerumInfo has key {
        /// Map of all markets created, keyed by their instrument quote pairs.
        marketMap: table::Table<string::String, address>,
    }

    /// Key used to map to a market address. Is first converted to a string using TypeInfo.
    struct MarketKey<phantom I, phantom Q> has key {}

    ///
    /// Entry functions.
    ///

    public entry fun init_ferum(owner: &signer) {
        let ownerAddr = address_of(owner);
        assert!(!exists<FerumInfo>(ownerAddr), ERR_NOT_ALLOWED);
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);
        move_to(owner, FerumInfo{
            marketMap: table::new<string::String, address>(),
        });
    }

    ///
    /// Public functions.
    ///

    public fun assert_ferum_inited() {
        assert!(exists<FerumInfo>(@ferum), ERR_NOT_ALLOWED);
    }

    public fun register_market<I, Q>(marketAddr: address) acquires FerumInfo {
        assert_ferum_inited();
        let info = borrow_global_mut<FerumInfo>(@ferum);
        let key = market_key<I, Q>();
        assert!(!table::contains(&info.marketMap, key), ERR_MARKET_EXISTS);
        table::add(&mut info.marketMap, market_key<I, Q>(), marketAddr);
    }

    public fun get_market_addr<I, Q>(): address acquires FerumInfo {
        assert_ferum_inited();
        let info = borrow_global<FerumInfo>(@ferum);
        let key = market_key<I, Q>();
        assert!(table::contains(&info.marketMap, key), ERR_MARKET_NOT_EXISTS);
        *table::borrow(&info.marketMap, key)
    }

    ///
    /// Private functions.
    ///

    fun market_key<I, Q>(): string::String {
        type_info::type_name<MarketKey<I, Q>>()
    }

    //
    // Tests
    //

    #[test(owner = @ferum)]
    fun test_init_ferum(owner: &signer) {
        // Tests that an account can init ferum.

        init_ferum(owner);
    }

    #[test(owner = @0x1)]
    #[expected_failure]
    fun test_init_not_ferum(owner: &signer) {
        // Tests that an account that's not ferum can't init.

        init_ferum(owner);
    }

    #[test(owner = @ferum, other = @0x2)]
    fun test_register_market(owner: &signer, other: &signer) acquires FerumInfo {
        // Tests that a market can be registered.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(other));
        init_ferum(owner);
        coin_test_helpers::setup_fake_coins(owner, other, 100, 18);
        register_market<coin_test_helpers::FMA, coin_test_helpers::FMB>(address_of(owner));
        let market_addr = get_market_addr<coin_test_helpers::FMA, coin_test_helpers::FMB>();
        assert!(market_addr == address_of(owner), 0);
    }

    #[test(owner = @ferum, other = @0x2)]
    #[expected_failure]
    fun test_register_other_combination(owner: &signer, other: &signer) acquires FerumInfo {
        // Tests that when market<I, Q> is registered, market<Q, I> is not.

        init_ferum(owner);
        coin_test_helpers::setup_fake_coins(owner, other, 100, 18);
        register_market<coin_test_helpers::FMA, coin_test_helpers::FMB>(address_of(owner));
        let market_addr = get_market_addr<coin_test_helpers::FMA, coin_test_helpers::FMB>();
        assert!(market_addr == address_of(owner), 0);
        get_market_addr<coin_test_helpers::FMB, coin_test_helpers::FMA>();
    }
}