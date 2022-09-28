module ferum::fees {
    use ferum_std::fixed_point_64::{Self, FixedPoint64};
    use ferum::sorted_map::{Self, SortedMap};

    //
    // Errors.
    //

    const ERR_NOT_ALLOWED: u64 = 1;
    const ERR_INVALID_MIN_FERUM_TOKEN_AMT: u64 = 2;
    const ERR_INVALID_FEE_STRUCTURE: u64 = 3;



    // Differing teirs users can qualify for.
    struct UserFeeTier has store, drop {
        // Minimum FER a user needs to hold to qualify for this fee tier.
        minFerumTokens: u64,
        // Fee charged to user trading in this fee tier (if the order is a taker).
        makerFeeBps: FixedPoint64,
        // Fee charged to user trading in this fee tier (if the order is a maker).
        takerFeeBps: FixedPoint64,
    }

    // Differing teirs protocols can qualify for.
    struct ProtocolFeeTier has store, drop {
        // Minimum FER a protocol needs to hold to qualify for this fee tier.
        minFerumTokens: u64,
        // Percentage of the user trading fee that is given to protocols that are in this fee tier.
        protocolFeeBps: FixedPoint64,
    }

    // Differing teirs LPs can qualify for.
    struct LPFeeTier has store, drop {
        // Minimum FER an LP needs to hold to qualify for this fee tier.
        minFerumTokens: u64,
        // Percentage of the user trading fee that is given to LPs that are in this fee tier.
        lpFeeBps: FixedPoint64,
    }

    // FeeStructure is a per market object. This allows fees to be customized for each market individually.
    struct FeeStructure has store {
        // List of UserFeeTiers sorted in increasing order.
        userTiers: SortedMap<UserFeeTier>,

        // List of ProtocolFeeTiers sorted in increasing order.
        protocolTiers: SortedMap<ProtocolFeeTier>,

        // List of LPFeeTiers sorted in increasing order.
        lpTiers: SortedMap<LPFeeTier>,

        //
        // User facing fees.
        //

        // Fee charged to user trading (if the order is a taker).
        takerFeeBps: FixedPoint64,
        // Fee charged to user trading (if the order is a maker).
        makerFeeBps: FixedPoint64,

        //
        // These fees are percentages of the user facing fee.
        //

        // Percentage of the user trading fee that is given to protocols.
        protocolFeeBps: FixedPoint64,
        // Only used for AMM pool. Percentage of user trading fee given to LPs.
        lpFeeBps: FixedPoint64,

        // Ferum takes 100% - protocol fee - LP fee.
    }

    // Returns a new fee structure.
    public fun new_structure(): FeeStructure {
        let structure = FeeStructure {
            userTiers: sorted_map::new_incr(),
            protocolTiers: sorted_map::new_incr(),
            lpTiers: sorted_map::new_incr(),

            takerFeeBps: fixed_point_64::zero(),
            makerFeeBps: fixed_point_64::zero(),

            protocolFeeBps: fixed_point_64::zero(),
            lpFeeBps: fixed_point_64::zero(),
        };

        structure
    }

    public fun new_structure_with_defaults(
        takerFee: FixedPoint64,
        makerFee: FixedPoint64,
        protocolFee: FixedPoint64,
        lpFee: FixedPoint64,
    ): FeeStructure {
        let structure = new_structure();

        set_default_user_fees(&mut structure, takerFee, makerFee);
        set_default_protocol_fee(&mut structure, protocolFee);
        set_default_lp_fee(&mut structure, lpFee);

        validate_fees(&structure);
        structure
    }

    // Sets the default % protocols takes from the fee charged to users.
    public fun set_default_protocol_fee(structure: &mut FeeStructure, fee: FixedPoint64) {
        structure.protocolFeeBps = fee;
        validate_fees(structure);
    }

    // Sets the default % LPs takes from the fee charged to users for AMM fills.
    public fun set_default_lp_fee(structure: &mut FeeStructure, fee: FixedPoint64) {
        structure.lpFeeBps = fee;
        validate_fees(structure);
    }

    // Sets the default fee for taker and maker orders.
    public fun set_default_user_fees(structure: &mut FeeStructure, taker: FixedPoint64, maker: FixedPoint64) {
        structure.makerFeeBps = maker;
        structure.takerFeeBps = taker;
        validate_fees(structure);
    }

    // Returns the % protocols get from user fees based on the protocol's fee tier.
    public fun get_protocol_fee_bps(structure: &FeeStructure, tokenHoldingsAmt: u64): FixedPoint64 {
        let tier = sorted_map::find_prev_value_ref(
            &structure.protocolTiers,
            (tokenHoldingsAmt as u128),
            &ProtocolFeeTier{
                minFerumTokens: 0,
                protocolFeeBps: structure.protocolFeeBps,
            },
        );
        tier.protocolFeeBps
    }

    // Returns the % LPs get from user fees based on the LP's fee tier.
    public fun get_lp_fee_bps(structure: &FeeStructure, tokenHoldingsAmt: u64): FixedPoint64 {
        let tier = sorted_map::find_prev_value_ref(
            &structure.lpTiers,
            (tokenHoldingsAmt as u128),
            &LPFeeTier{
                minFerumTokens: 0,
                lpFeeBps: structure.lpFeeBps,
            },
        );
        tier.lpFeeBps
    }

    // Returns (taker, maker) fees for users based on the user's token holdings.
    public fun get_user_fee_bps(structure: &FeeStructure, tokenHoldingsAmt: u64): (FixedPoint64, FixedPoint64) {
        let tier = sorted_map::find_prev_value_ref(
            &structure.userTiers,
            (tokenHoldingsAmt as u128),
            &UserFeeTier{
                minFerumTokens: 0,
                takerFeeBps: structure.takerFeeBps,
                makerFeeBps: structure.makerFeeBps,
            },
        );
        (tier.takerFeeBps, tier.makerFeeBps)
    }

    public fun set_user_fee_tier(
        structure: &mut FeeStructure,
        minFerumTokens: u64,
        takerFeeBps: FixedPoint64,
        makerFeeBps: FixedPoint64,
    ) {
        let tier = UserFeeTier {
            minFerumTokens,
            makerFeeBps,
            takerFeeBps,
        };
        sorted_map::add(&mut structure.userTiers, (minFerumTokens as u128), tier);
        validate_fees(structure);
    }

    public fun set_protocol_fee_tier(
        structure: &mut FeeStructure,
        minFerumTokens: u64,
        protocolFeeBps: FixedPoint64,
    ) {
        let tier = ProtocolFeeTier {
            minFerumTokens,
            protocolFeeBps,
        };
        sorted_map::add(&mut structure.protocolTiers, (minFerumTokens as u128), tier);
        validate_fees(structure);
    }

    public fun set_lp_fee_tier(
        structure: &mut FeeStructure,
        minFerumTokens: u64,
        lpFeeBps: FixedPoint64,
    ) {
        let tier = LPFeeTier {
            minFerumTokens,
            lpFeeBps,
        };
        sorted_map::add(&mut structure.lpTiers, (minFerumTokens as u128), tier);
        validate_fees(structure);
    }

    public fun remove_user_fee_tier(structure: &mut FeeStructure, minFerumTokens: u64) {
        sorted_map::remove(&mut structure.userTiers, (minFerumTokens as u128));
    }

    public fun remove_protocol_fee_tier(structure: &mut FeeStructure, minFerumTokens: u64) {
        sorted_map::remove(&mut structure.protocolTiers, (minFerumTokens as u128));
    }

    public fun remove_lp_fee_tier(structure: &mut FeeStructure, minFerumTokens: u64) {
        sorted_map::remove(&mut structure.lpTiers, (minFerumTokens as u128));
    }

    fun validate_fees(structure: &FeeStructure) {
        let hundred = fixed_point_64::from_u128(1, 0);

        let maxProtocolFee = get_max_protocol_fee(structure);
        let maxLPFee = get_max_protocol_fee(structure);

        // Assert that for all tiers, lp and protocol tiers don't add up to be greater than 100%.
        let sum = fixed_point_64::add(maxProtocolFee, maxLPFee);
        assert!(fixed_point_64::lte(sum, hundred), ERR_INVALID_FEE_STRUCTURE);

        // Assert that user fees don't exceed 100.
        let it = sorted_map::key_iterator(&structure.userTiers);
        while (sorted_map::has_next_key(&it)) {
            let key = sorted_map::get_next_key(&structure.userTiers, &mut it);
            let tier = sorted_map::get_ref(&structure.userTiers, key);
            assert!(fixed_point_64::lte(tier.makerFeeBps, hundred), ERR_INVALID_FEE_STRUCTURE);
            assert!(fixed_point_64::lte(tier.takerFeeBps, hundred), ERR_INVALID_FEE_STRUCTURE);
        };
        assert!(fixed_point_64::lte(structure.makerFeeBps, hundred), ERR_INVALID_FEE_STRUCTURE);
        assert!(fixed_point_64::lte(structure.takerFeeBps, hundred), ERR_INVALID_FEE_STRUCTURE);
    }

    fun get_max_protocol_fee(structure: &FeeStructure): FixedPoint64 {
        let maxFee = fixed_point_64::zero();
        let it = sorted_map::key_iterator(&structure.protocolTiers);
        while (sorted_map::has_next_key(&it)) {
            let key = sorted_map::get_next_key(&structure.protocolTiers, &mut it);
            let tier = sorted_map::get_ref(&structure.protocolTiers, key);
            if (fixed_point_64::gt(tier.protocolFeeBps, maxFee)) {
                maxFee = tier.protocolFeeBps;
            }
        };
        maxFee
    }

    fun get_max_lp_fee(structure: &FeeStructure): FixedPoint64 {
        let maxFee = fixed_point_64::zero();
        let it = sorted_map::key_iterator(&structure.lpTiers);
        while (sorted_map::has_next_key(&it)) {
            let key = sorted_map::get_next_key(&structure.lpTiers, &mut it);
            let tier = sorted_map::get_ref(&structure.lpTiers, key);
            if (fixed_point_64::gt(tier.lpFeeBps, maxFee)) {
                maxFee = tier.lpFeeBps;
            }
        };
        maxFee
    }

    #[test]
    fun test_protocol_fee_tiers() {
        let structure = new_structure();
        // Add some protocol tiers.
        set_protocol_fee_tier(
            &mut structure,
            100,
            fixed_point_64::from_u128(10, 4),
        );
        set_protocol_fee_tier(
            &mut structure,
            200,
            fixed_point_64::from_u128(8, 4),
        );
        set_protocol_fee_tier(
            &mut structure,
            125,
            fixed_point_64::from_u128(9, 4),
        );

        set_default_protocol_fee(&mut structure, fixed_point_64::from_u128(15, 4));

        let fee = get_protocol_fee_bps(&structure, 0);
        assert!(fixed_point_64::eq(fee, fixed_point_64::from_u128(15, 4)), 0);

        let fee = get_protocol_fee_bps(&structure, 50);
        assert!(fixed_point_64::eq(fee, fixed_point_64::from_u128(15, 4)), 0);

        let fee = get_protocol_fee_bps(&structure, 100);
        assert!(fixed_point_64::eq(fee, fixed_point_64::from_u128(10, 4)), 0);

        let fee = get_protocol_fee_bps(&structure, 1000);
        assert!(fixed_point_64::eq(fee, fixed_point_64::from_u128(8, 4)), 0);

        drop_structure(structure);
    }

    #[test]
    fun test_lp_fee_tiers() {
        let structure = new_structure();
        // Add some lp tiers.
        set_lp_fee_tier(
            &mut structure,
            100,
            fixed_point_64::from_u128(10, 4),
        );
        set_lp_fee_tier(
            &mut structure,
            200,
            fixed_point_64::from_u128(8, 4),
        );
        set_lp_fee_tier(
            &mut structure,
            125,
            fixed_point_64::from_u128(9, 4),
        );

        set_default_lp_fee(&mut structure, fixed_point_64::from_u128(15, 4));

        let fee = get_lp_fee_bps(&structure, 0);
        assert!(fixed_point_64::eq(fee, fixed_point_64::from_u128(15, 4)), 0);

        let fee = get_lp_fee_bps(&structure, 50);
        assert!(fixed_point_64::eq(fee, fixed_point_64::from_u128(15, 4)), 0);

        let fee = get_lp_fee_bps(&structure, 100);
        assert!(fixed_point_64::eq(fee, fixed_point_64::from_u128(10, 4)), 0);

        let fee = get_lp_fee_bps(&structure, 1000);
        assert!(fixed_point_64::eq(fee, fixed_point_64::from_u128(8, 4)), 0);

        drop_structure(structure);
    }

    #[test]
    fun test_user_fee_tiers() {
        let structure = new_structure();
        // Add some user tiers.
        set_user_fee_tier(
            &mut structure,
            100,
            fixed_point_64::from_u128(20, 4),
            fixed_point_64::from_u128(3, 4),
        );
        set_user_fee_tier(
            &mut structure,
            200,
            fixed_point_64::from_u128(10, 4),
            fixed_point_64::from_u128(1, 4),
        );
        set_user_fee_tier(
            &mut structure,
            150,
            fixed_point_64::from_u128(15, 4),
            fixed_point_64::from_u128(2, 4),
        );
        set_default_user_fees(
            &mut structure,
            fixed_point_64::from_u128(25, 4),
            fixed_point_64::from_u128(4, 4),
        );

        let (taker, maker) = get_user_fee_bps(&structure, 0);
        assert!(fixed_point_64::eq(taker, fixed_point_64::from_u128(25, 4)), 0);
        assert!(fixed_point_64::eq(maker, fixed_point_64::from_u128(4, 4)), 0);

        let (taker, maker) = get_user_fee_bps(&structure, 75);
        assert!(fixed_point_64::eq(taker, fixed_point_64::from_u128(25, 4)), 0);
        assert!(fixed_point_64::eq(maker, fixed_point_64::from_u128(4, 4)), 0);

        let (taker, maker) = get_user_fee_bps(&structure, 100);
        assert!(fixed_point_64::eq(taker, fixed_point_64::from_u128(20, 4)), 0);
        assert!(fixed_point_64::eq(maker, fixed_point_64::from_u128(3, 4)), 0);

        let (taker, maker) = get_user_fee_bps(&structure, 125);
        assert!(fixed_point_64::eq(taker, fixed_point_64::from_u128(20, 4)), 0);
        assert!(fixed_point_64::eq(maker, fixed_point_64::from_u128(3, 4)), 0);

        let (taker, maker) = get_user_fee_bps(&structure, 150);
        assert!(fixed_point_64::eq(taker, fixed_point_64::from_u128(15, 4)), 0);
        assert!(fixed_point_64::eq(maker, fixed_point_64::from_u128(2, 4)), 0);

        let (taker, maker) = get_user_fee_bps(&structure, 1500);
        assert!(fixed_point_64::eq(taker, fixed_point_64::from_u128(10, 4)), 0);
        assert!(fixed_point_64::eq(maker, fixed_point_64::from_u128(1, 4)), 0);

        drop_structure(structure);
    }

    #[test]
    #[expected_faulure]
    fun test_invalid_default_protocol_fee() {
        let structure = new_structure();
        set_default_protocol_fee(&mut structure, fixed_point_64::from_u128(100, 0));
        drop_structure(structure);
    }

    #[test]
    #[expected_faulure]
    fun test_invalid_lp_fee_with_default_protocol_fee() {
        let structure = new_structure();
        set_default_protocol_fee(&mut structure, fixed_point_64::from_u128(1, 0));
        set_lp_fee_tier(
            &mut structure,
            1,
            fixed_point_64::from_u128(1, 4),
        );
        drop_structure(structure);
    }

    #[test]
    #[expected_faulure]
    fun test_invalid_default_lp_fee() {
        let structure = new_structure();
        set_default_lp_fee(&mut structure, fixed_point_64::from_u128(100, 0));
        drop_structure(structure);
    }

    #[test]
    #[expected_faulure]
    fun test_invalid_protocol_fee_with_default_lp_fee() {
        let structure = new_structure();
        set_default_lp_fee(&mut structure, fixed_point_64::from_u128(1, 0));
        set_protocol_fee_tier(
            &mut structure,
            1,
            fixed_point_64::from_u128(1, 4),
        );
        drop_structure(structure);
    }

    #[test]
    #[expected_faulure]
    fun test_invalid_protocol_fee_with_lp_fee_tier() {
        let structure = new_structure();
        set_lp_fee_tier(
            &mut structure,
            1,
            fixed_point_64::from_u128(9999, 4),
        );
        set_protocol_fee_tier(
            &mut structure,
            1,
            fixed_point_64::from_u128(2, 4),
        );
        drop_structure(structure);
    }

    #[test]
    #[expected_failure]
    fun test_invalid_default_user_fees() {
        let structure = new_structure();
        set_default_user_fees(
            &mut structure,
            fixed_point_64::from_u128(2, 0),
            fixed_point_64::from_u128(0, 0),
        );
        drop_structure(structure);
    }

    #[test]
    #[expected_failure]
    fun test_invalid_tier_user_fees() {
        let structure = new_structure();
        set_user_fee_tier(
            &mut structure,
            100,
            fixed_point_64::from_u128(2, 0),
            fixed_point_64::from_u128(0, 0),
        );
        drop_structure(structure);
    }

    #[test_only]
    fun drop_structure(structure: FeeStructure) {
        let FeeStructure {
            userTiers,
            protocolTiers,
            lpTiers,
            takerFeeBps: _,
            makerFeeBps: _,
            protocolFeeBps: _,
            lpFeeBps: _,
        } = structure;

        sorted_map::empty_and_drop_map(userTiers);
        sorted_map::empty_and_drop_map(protocolTiers);
        sorted_map::empty_and_drop_map(lpTiers);
    }
}