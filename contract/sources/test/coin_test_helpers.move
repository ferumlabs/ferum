module ferum::coin_test_helpers {
    #[test_only]
    use aptos_framework::coin;
    #[test_only]
    use std::string;
    #[test_only]
    use std::signer::address_of;

    #[test_only]
    struct FMA {}

    #[test_only]
    struct FMB {}

    #[test_only]
    struct FakeMoneyACapabilities has key {
        burn: coin::BurnCapability<FMA>,
        freeze: coin::FreezeCapability<FMA>,
        mint: coin::MintCapability<FMA>,
    }

    #[test_only]
    struct FakeMoneyBCapabilities has key {
        burn: coin::BurnCapability<FMB>,
        freeze: coin::FreezeCapability<FMB>,
        mint: coin::MintCapability<FMB>,
    }

    #[test_only]
    public fun deposit_fake_coins(
        owner: &signer,
        amt: u64,
        user: &signer,
    ) acquires FakeMoneyACapabilities, FakeMoneyBCapabilities {
        register_and_deposit_fma(owner, user, amt);
        register_and_deposit_fmb(owner, user, amt);
    }

    #[test_only]
    public fun create_fake_coins(owner: &signer, decimals: u8) {
        let (
            burn,
            freeze,
            mint
        ) = coin::initialize<FMA>(
            owner,
            string::utf8(b"Fake Money A"),
            string::utf8(b"FMA"),
            decimals,
            true
        );
        move_to(owner, FakeMoneyACapabilities { burn,  freeze,  mint });

        let (
            burn,
            freeze,
            mint
        ) = coin::initialize<FMB>(
            owner,
            string::utf8(b"Fake Money B"),
            string::utf8(b"FMB"),
            decimals,
            true
        );
        move_to(owner, FakeMoneyBCapabilities { burn, freeze, mint });
    }

    #[test_only]
    public fun register_and_deposit_fma(owner: &signer, user: &signer, amt: u64) acquires FakeMoneyACapabilities {
        coin::register<FMA>(user);
        deposit_fma(owner, user, amt);
    }

    #[test_only]
    public fun register_and_deposit_fmb(owner: &signer, user: &signer, amt: u64) acquires FakeMoneyBCapabilities {
        coin::register<FMB>(user);
        deposit_fmb(owner, user, amt);
    }

    #[test_only]
    public fun deposit_fma(owner: &signer, user: &signer, amt: u64) acquires FakeMoneyACapabilities {
        let cap = borrow_global<FakeMoneyACapabilities>(address_of(owner));
        coin::deposit(address_of(user), coin::mint(amt, &cap.mint));
    }

    #[test_only]
    public fun deposit_fmb(owner: &signer, user: &signer, amt: u64) acquires FakeMoneyBCapabilities {
        let cap = borrow_global<FakeMoneyBCapabilities>(address_of(owner));
        coin::deposit(address_of(user), coin::mint(amt, &cap.mint));
    }
}