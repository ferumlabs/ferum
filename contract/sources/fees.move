module ferum::fees {
    use std::vector;

    //
    // Errors.
    //

    const ERR_NOT_ALLOWED: u64 = 300;
    const ERR_INVALID_FEE_STRUCTURE: u64 = 301;

    // Differing teirs users can qualify for.
    struct UserFeeTier has store, drop {
        // Fee charged to user trading in this fee tier (if the order is a taker).
        makerFeeBps: u64,
        // Fee charged to user trading in this fee tier (if the order is a maker).
        takerFeeBps: u64,
    }

    // Differing teirs protocols can qualify for.
    struct ProtocolFeeTier has store, drop {
        // Percentage of the user trading fee that is given to protocols that are in this fee tier.
        protocolFeeBps: u64,
    }

    // Tier structure encapsulating different types of tiers.
    struct Tier<T: store + drop> has store, drop {
        // Minimum Fe a protocol needs to hold to qualify for this fee tier.
        minFerumTokens: u64,
        // Information about this tier.
        value: T,
    }

    // FeeTiers is a per market object. This allows fees to be customized for each market individually.
    struct FeeTiers has store, drop {
        // List of UserFeeTiers sorted in increasing order.
        userTiers: vector<Tier<UserFeeTier>>,
        // List of ProtocolFeeTiers sorted in increasing order.
        protocolTiers: vector<Tier<ProtocolFeeTier>>,

        // Ferum takes 100% - protocol fee.
    }

    public fun new_tiers_with_defaults(takerFeeBps: u64, makerFeeBps: u64, protocolFeeBps: u64): FeeTiers {
        let structure = FeeTiers {
            userTiers: vector[
                Tier{
                    minFerumTokens: 0,
                    value: UserFeeTier {
                        makerFeeBps,
                        takerFeeBps,
                    },
                },
            ],
            protocolTiers: vector[
                Tier{
                    minFerumTokens: 0,
                    value: ProtocolFeeTier {
                        protocolFeeBps,
                    },
                },
            ],
        };
        validate_fees(&structure);
        structure
    }

    // Returns the % protocols get from user fees based on the protocol's fee tier.
    public fun get_protocol_fee_bps(structure: &FeeTiers, tokenHoldingsAmt: u64): u64 {
        let tier = find_tier<ProtocolFeeTier>(&structure.protocolTiers, tokenHoldingsAmt);
        tier.value.protocolFeeBps
    }

    // Returns (taker, maker) fees for users based on the user's token holdings.
    public fun get_user_fee_bps(structure: &FeeTiers, tokenHoldingsAmt: u64): (u64, u64) {
        let tier = find_tier<UserFeeTier>(&structure.userTiers, tokenHoldingsAmt);
        (tier.value.takerFeeBps, tier.value.makerFeeBps)
    }

    public fun set_user_fee_tier(
        structure: &mut FeeTiers,
        minFerumTokens: u64,
        takerFeeBps: u64,
        makerFeeBps: u64,
    ) {
        let tier = Tier {
            value: UserFeeTier {
                makerFeeBps,
                takerFeeBps,
            },
            minFerumTokens,
        };
        set_tier<UserFeeTier>(&mut structure.userTiers, tier);
        validate_fees(structure);
    }

    public fun set_protocol_fee_tier(
        structure: &mut FeeTiers,
        minFerumTokens: u64,
        protocolFeeBps: u64,
    ) {
        let tier = Tier {
            minFerumTokens,
            value: ProtocolFeeTier {
                protocolFeeBps,
            },
        };
        set_tier<ProtocolFeeTier>(&mut structure.protocolTiers, tier);
        validate_fees(structure);
    }

    public fun remove_user_fee_tier(structure: &mut FeeTiers, minFerumTokens: u64) {
        remove_tier<UserFeeTier>(&mut structure.userTiers, minFerumTokens);
    }

    public fun remove_protocol_fee_tier(structure: &mut FeeTiers, minFerumTokens: u64) {
        remove_tier<ProtocolFeeTier>(&mut structure.protocolTiers, minFerumTokens);
    }


    inline fun validate_fees(structure: &FeeTiers) {
        let hundred = 10000000000;
        let bip = 1000000;
        let percent = 100000000;

        let protocolFeeCount = vector::length(&structure.protocolTiers);
        let i = 0;
        while (i < protocolFeeCount) {
            let tier = vector::borrow(&structure.protocolTiers, i);
            if (tier.value.protocolFeeBps != 0) {
                assert!(tier.value.protocolFeeBps >= percent, ERR_INVALID_FEE_STRUCTURE);
            };
            assert!(tier.value.protocolFeeBps <= hundred, ERR_INVALID_FEE_STRUCTURE);
            i = i + 1;
        };

        // Assert that user fees don't exceed 100.
        let i = 0;
        let size = vector::length(&structure.userTiers);
        while (i < size) {
            let tier = vector::borrow(&structure.userTiers, i);
            assert!(tier.value.makerFeeBps < hundred, ERR_INVALID_FEE_STRUCTURE);
            if (tier.value.makerFeeBps != 0) {
                assert!(tier.value.makerFeeBps >= bip, ERR_INVALID_FEE_STRUCTURE);
            };
            assert!(tier.value.takerFeeBps < hundred, ERR_INVALID_FEE_STRUCTURE);
            if (tier.value.takerFeeBps != 0) {
                assert!(tier.value.takerFeeBps >= bip, ERR_INVALID_FEE_STRUCTURE);
            };
            i = i + 1;
        };
    }

    // TODO: make inline once bugs are fixed.
    fun set_tier<T: store + drop>(list: &mut vector<Tier<T>>, tier: Tier<T>) {
        let i = 0;
        let size = vector::length(list);
        let tierMinFe = tier.minFerumTokens;
        assert!(size > 0, ERR_INVALID_FEE_STRUCTURE);
        while (i < size) {
            let curr = vector::borrow_mut(list, i);
            if (curr.minFerumTokens == tierMinFe) {
                *curr = tier;
                return
            };
            if (curr.minFerumTokens > tierMinFe) {
                break
            };
            i = i + 1;
        };
        vector::push_back(list, tier);
        while (i < size) {
            vector::swap(list, i, size);
            i = i + 1;
        };
    }

    inline fun remove_tier<T: store + drop>(list: &mut vector<Tier<T>>, minFerumTokens: u64) {
        let i = 0;
        let size = vector::length(list);
        assert!(size > 0, ERR_INVALID_FEE_STRUCTURE);
        while (i < size) {
            let curr = vector::borrow_mut(list, i);
            if (curr.minFerumTokens == minFerumTokens) {
                break
            };
            i = i + 1;
        };
        while (i < size - 1) {
            vector::swap(list, i, i+1);
            i = i + 1;
        };
        vector::pop_back(list);
    }

    inline fun find_tier<T: store + drop>(list: &vector<Tier<T>>, val: u64): &Tier<T> {
        let size = vector::length(list);
        assert!(size > 0, ERR_INVALID_FEE_STRUCTURE);
        let i = 1;
        while (i < size) {
            let curr = vector::borrow(list, i);
            if (curr.minFerumTokens > val) {
                break
            };
            i = i + 1;
        };
        vector::borrow(list, i - 1)
    }

    #[test]
    fun test_protocol_fee_tiers() {
        let structure = new_tiers_with_defaults(5000000, 0, 500000000);
        // Add some protocol tiers.
        set_protocol_fee_tier(
            &mut structure,
            100,
            1000000000,
        );
        set_protocol_fee_tier(
            &mut structure,
            200,
            2000000000,
        );
        set_protocol_fee_tier(
            &mut structure,
            125,
            1600000000,
        );
        set_protocol_fee_tier(
            &mut structure,
            125,
            1500000000,
        );
        set_protocol_fee_tier(
            &mut structure,
            25,
            1600000000,
        );
        remove_protocol_fee_tier(&mut structure, 25);

        let fee = get_protocol_fee_bps(&structure, 0);
        assert!(fee == 500000000, 0);
        let fee = get_protocol_fee_bps(&structure, 50);
        assert!(fee == 500000000, 0);
        let fee = get_protocol_fee_bps(&structure, 130);
        assert!(fee == 1500000000, 0);
        let fee = get_protocol_fee_bps(&structure, 1000);
        assert!(fee == 2000000000, 0);
    }

    #[test]
    fun test_user_fee_tiers() {
        let structure = new_tiers_with_defaults(25000000, 4000000, 500000000);
        // Add some user tiers.
        set_user_fee_tier(
            &mut structure,
            100,
            20000000,
            3000000,
        );
        set_user_fee_tier(
            &mut structure,
            200,
            10000000,
            1000000,
        );
        set_user_fee_tier(
            &mut structure,
            150,
            18000000,
            5000000,
        );
        set_user_fee_tier(
            &mut structure,
            150,
            15000000,
            2000000,
        );
        set_user_fee_tier(
            &mut structure,
            20,
            19000000,
            2000000,
        );
        remove_user_fee_tier(&mut structure, 20);

        let (taker, maker) = get_user_fee_bps(&structure, 0);
        assert!(taker == 25000000, 0);
        assert!(maker == 4000000, 0);
        let (taker, maker) = get_user_fee_bps(&structure, 75);
        assert!(taker == 25000000, 0);
        assert!(maker == 4000000, 0);
        let (taker, maker) = get_user_fee_bps(&structure, 100);
        assert!(taker == 20000000, 0);
        assert!(maker == 3000000, 0);
        let (taker, maker) = get_user_fee_bps(&structure, 125);
        assert!(taker == 20000000, 0);
        assert!(maker == 3000000, 0);
        let (taker, maker) = get_user_fee_bps(&structure, 150);
        assert!(taker == 15000000, 0);
        assert!(maker == 2000000, 0);
        let (taker, maker) = get_user_fee_bps(&structure, 1500);
        assert!(taker == 10000000, 0);
        assert!(maker == 1000000, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_invalid_default_protocol_fee_max() {
        new_tiers_with_defaults(0, 0, 20000000000);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_invalid_default_protocol_fee_min() {
        new_tiers_with_defaults(0, 0, 1000000);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_invalid_default_user_taker_fee_max() {
        new_tiers_with_defaults(20000000000, 0, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_invalid_default_user_maker_fee_max() {
        new_tiers_with_defaults(0, 20000000000, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_invalid_default_user_taker_fee_min() {
        new_tiers_with_defaults(100000, 0, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_invalid_default_user_maker_fee_min() {
        new_tiers_with_defaults(0, 100000, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_invalid_tier_user_fees() {
        let structure = new_tiers_with_defaults(0, 0, 0);
        set_user_fee_tier(
            &mut structure,
            100,
            1000,
            0,
        );
    }

    #[test]
    #[expected_failure(abort_code=ERR_INVALID_FEE_STRUCTURE)]
    fun test_invalid_tier_protocol_fees() {
        let structure = new_tiers_with_defaults(0, 0, 0);
        set_protocol_fee_tier(
            &mut structure,
            100,
            1000,
        );
    }
}