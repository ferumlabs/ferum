module ferum::admin {
    use aptos_std::table;
    use std::signer::address_of;
    use std::string;
    use aptos_std::type_info;
    use ferum::fees::{Self, FeeStructure};
    use ferum_std::fixed_point_64;
    #[test_only]
    use ferum::coin_test_helpers::{Self, FMA, FMB};
    #[test_only]
    use aptos_framework::account;
    #[test_only]
    use ferum::market_types::{LOB};

    //
    // Enums
    //

    // Used to identify a market with the default fee type.
    const FEE_TYPE_DEFAULT: u8 = 1;
    // Used to identify a market with the stable swap fee type.
    const FEE_TYPE_STABLE_SWAP: u8 = 2;

    //
    // Errors
    //

    // Admin errors reserve [200, 299].

    const ERR_NOT_ALLOWED: u64 = 200;
    const ERR_MARKET_NOT_EXISTS: u64 = 201;
    const ERR_MARKET_EXISTS: u64 = 202;
    const ERR_INVALID_FEE_TYPE: u64 = 203;

    //
    // Structs.
    //

    // Global info object for ferum.
    struct FerumInfo has key {
        // Map of all markets created, keyed by their instrument quote pairs.
        marketMap: table::Table<string::String, address>,
        // Default fee structure for all Ferum market types.
        fees: Fees,
    }

    // Essentially a map of all fee types to FeeStructures.
    struct Fees has store {
        stable: FeeStructure,
        default: FeeStructure,
    }

    // Key used to map to a market address. Is first converted to a string using TypeInfo.
    struct MarketKey<phantom I, phantom Q, phantom T> has key {}

    //
    // Entry functions.
    //

    // All fee values are fixed points with 4 decimal places.
    public entry fun init_ferum(
        owner: &signer,
        defaultMakerFeeRaw: u128,
        defaultTakerFeeRaw: u128,
        defaultProtocolFeeRaw: u128,
        defaultLPFeeRaw: u128,
    ) {
        let ownerAddr = address_of(owner);
        assert!(!exists<FerumInfo>(ownerAddr), ERR_NOT_ALLOWED);
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);

        // Fees converted to fixed points.
        let defaultMakerFee = fixed_point_64::from_u128(defaultMakerFeeRaw, 4);
        let defaultTakerFee = fixed_point_64::from_u128(defaultTakerFeeRaw, 4);
        let defaultProtocolFee = fixed_point_64::from_u128(defaultProtocolFeeRaw, 4);
        let defaultLPFee = fixed_point_64::from_u128(defaultLPFeeRaw, 4);

        // Create fee structure.
        let fees = Fees{
            default: fees::new_structure_with_defaults(
                defaultTakerFee,
                defaultMakerFee,
                defaultProtocolFee,
                defaultLPFee,
            ),
            stable: fees::new_structure_with_defaults(
                defaultTakerFee,
                defaultMakerFee,
                defaultProtocolFee,
                defaultLPFee,
            ),
        };

        move_to(owner, FerumInfo {
            marketMap: table::new<string::String, address>(),
            fees,
        });
    }

    // Fee values are fixed points with 4 decimal places.
    public entry fun add_protocol_fee_tier(
        owner: &signer,
        feeType: u8,
        minFerumTokenHoldings: u64,
        feeRaw: u128,
    ) acquires FerumInfo {
        let ownerAddr = address_of(owner);
        assert!(!exists<FerumInfo>(ownerAddr), ERR_NOT_ALLOWED);
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);

        let fee = fixed_point_64::from_u128(feeRaw, 4);
        let info = borrow_global_mut<FerumInfo>(@ferum);

        if (feeType == FEE_TYPE_DEFAULT) {
            fees::set_protocol_fee_tier(&mut info.fees.default, minFerumTokenHoldings, fee)
        } else if (feeType == FEE_TYPE_STABLE_SWAP) {
            fees::set_protocol_fee_tier(&mut info.fees.stable, minFerumTokenHoldings, fee)
        } else {
            abort ERR_INVALID_FEE_TYPE
        }
    }

    // Fee values are fixed points with 4 decimal places.
    public entry fun add_lp_fee_tier(
        owner: &signer,
        feeType: u8,
        minFerumTokenHoldings: u64,
        feeRaw: u128,
    ) acquires FerumInfo {
        let ownerAddr = address_of(owner);
        assert!(!exists<FerumInfo>(ownerAddr), ERR_NOT_ALLOWED);
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);

        let fee = fixed_point_64::from_u128(feeRaw, 4);
        let info = borrow_global_mut<FerumInfo>(@ferum);

        if (feeType == FEE_TYPE_DEFAULT) {
            fees::set_lp_fee_tier(&mut info.fees.default, minFerumTokenHoldings, fee)
        } else if (feeType == FEE_TYPE_STABLE_SWAP) {
            fees::set_lp_fee_tier(&mut info.fees.stable, minFerumTokenHoldings, fee)
        } else {
            abort ERR_INVALID_FEE_TYPE
        }
    }

    // Fee values are fixed points with 4 decimal places.
    public entry fun add_user_fee_tier(
        owner: &signer,
        feeType: u8,
        minFerumTokenHoldings: u64,
        takerFeeRaw: u128,
        makerFeeRaw: u128,
    ) acquires FerumInfo {
        let ownerAddr = address_of(owner);
        assert!(!exists<FerumInfo>(ownerAddr), ERR_NOT_ALLOWED);
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);

        let takerFee = fixed_point_64::from_u128(takerFeeRaw, 4);
        let makerFee = fixed_point_64::from_u128(makerFeeRaw, 4);
        let info = borrow_global_mut<FerumInfo>(@ferum);

        if (feeType == FEE_TYPE_DEFAULT) {
            fees::set_user_fee_tier(&mut info.fees.default, minFerumTokenHoldings, takerFee, makerFee)
        } else if (feeType == FEE_TYPE_STABLE_SWAP) {
            fees::set_user_fee_tier(&mut info.fees.stable, minFerumTokenHoldings, takerFee, makerFee)
        } else {
            abort ERR_INVALID_FEE_TYPE
        }
    }

    public entry fun remove_protocol_fee_tier(
        owner: &signer,
        feeType: u8,
        minFerumTokenHoldings: u64,
    ) acquires FerumInfo {
        let ownerAddr = address_of(owner);
        assert!(!exists<FerumInfo>(ownerAddr), ERR_NOT_ALLOWED);
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);
        let info = borrow_global_mut<FerumInfo>(@ferum);

        if (feeType == FEE_TYPE_DEFAULT) {
            fees::remove_protocol_fee_tier(&mut info.fees.default, minFerumTokenHoldings)
        } else if (feeType == FEE_TYPE_STABLE_SWAP) {
            fees::remove_protocol_fee_tier(&mut info.fees.stable, minFerumTokenHoldings)
        } else {
            abort ERR_INVALID_FEE_TYPE
        }
    }

    public entry fun remove_lp_fee_tier(
        owner: &signer,
        feeType: u8,
        minFerumTokenHoldings: u64,
    ) acquires FerumInfo {
        let ownerAddr = address_of(owner);
        assert!(!exists<FerumInfo>(ownerAddr), ERR_NOT_ALLOWED);
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);
        let info = borrow_global_mut<FerumInfo>(@ferum);

        if (feeType == FEE_TYPE_DEFAULT) {
            fees::remove_lp_fee_tier(&mut info.fees.default, minFerumTokenHoldings)
        } else if (feeType == FEE_TYPE_STABLE_SWAP) {
            fees::remove_lp_fee_tier(&mut info.fees.stable, minFerumTokenHoldings)
        } else {
            abort ERR_INVALID_FEE_TYPE
        }
    }

    public entry fun remove_user_fee_tier(
        owner: &signer,
        feeType: u8,
        minFerumTokenHoldings: u64,
    ) acquires FerumInfo {
        let ownerAddr = address_of(owner);
        assert!(!exists<FerumInfo>(ownerAddr), ERR_NOT_ALLOWED);
        assert!(ownerAddr == @ferum, ERR_NOT_ALLOWED);
        let info = borrow_global_mut<FerumInfo>(@ferum);

        if (feeType == FEE_TYPE_DEFAULT) {
            fees::remove_user_fee_tier(&mut info.fees.default, minFerumTokenHoldings)
        } else if (feeType == FEE_TYPE_STABLE_SWAP) {
            fees::remove_user_fee_tier(&mut info.fees.stable, minFerumTokenHoldings)
        } else {
            abort ERR_INVALID_FEE_TYPE
        }
    }

    //
    // Public functions.
    //

    public fun assert_ferum_inited() {
        assert!(exists<FerumInfo>(@ferum), ERR_NOT_ALLOWED);
    }

    public fun register_market<I, Q, T>(marketAddr: address) acquires FerumInfo {
        assert_ferum_inited();
        let info = borrow_global_mut<FerumInfo>(@ferum);
        let key = market_key<I, Q, T>();
        assert!(!table::contains(&info.marketMap, key), ERR_MARKET_EXISTS);
        table::add(&mut info.marketMap, market_key<I, Q, T>(), marketAddr);
    }

    public fun get_market_addr<I, Q, T>(): address acquires FerumInfo {
        assert_ferum_inited();
        let info = borrow_global<FerumInfo>(@ferum);
        let key = market_key<I, Q, T>();
        assert!(table::contains(&info.marketMap, key), ERR_MARKET_NOT_EXISTS);
        *table::borrow(&info.marketMap, key)
    }

    //
    // Private functions.
    //

    fun market_key<I, Q, T>(): string::String {
        type_info::type_name<MarketKey<I, Q, T>>()
    }

    //
    // Tests
    //

    #[test(owner = @ferum)]
    fun test_init_ferum(owner: &signer) {
        // Tests that an account can init ferum.

        init_ferum(owner, 0, 0, 0, 0);
    }

    #[test(owner = @0x1)]
    #[expected_failure]
    fun test_init_not_ferum(owner: &signer) {
        // Tests that an account that's not ferum can't init.

        init_ferum(owner, 0, 0, 0, 0);
    }

    #[test(owner = @ferum, other = @0x2)]
    fun test_register_market(owner: &signer, other: &signer) acquires FerumInfo {
        // Tests that a market can be registered.

        account::create_account_for_test(address_of(owner));
        account::create_account_for_test(address_of(other));
        init_ferum(owner, 0, 0, 0, 0);
        coin_test_helpers::setup_fake_coins(owner, other, 100, 18);
        register_market<FMA, FMB, LOB>(address_of(owner));
        let market_addr = get_market_addr<FMA, FMB, LOB>();
        assert!(market_addr == address_of(owner), 0);
    }

    #[test(owner = @ferum, other = @0x2)]
    #[expected_failure]
    fun test_register_other_combination(owner: &signer, other: &signer) acquires FerumInfo {
        // Tests that when market<I, Q> is registered, market<Q, I> is not.

        init_ferum(owner, 0, 0, 0, 0);
        coin_test_helpers::setup_fake_coins(owner, other, 100, 18);
        register_market<FMA, FMB, LOB>(address_of(owner));
        let market_addr = get_market_addr<FMA, FMB, LOB>();
        assert!(market_addr == address_of(owner), 0);
        get_market_addr<FMB, FMA, LOB>();
    }
}