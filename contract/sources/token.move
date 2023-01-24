module ferum::token {
    use std::signer::address_of;
    use aptos_framework::coin;
    use std::string;

    const ERR_NOT_ALLOWED: u64 = 1;

    // CoinType for the Ferum token.
    struct Fe {}

    // Capabiity store for Fe token.
    struct FeCapabilities has key {
        burn: coin::BurnCapability<Fe>,
        freeze: coin::FreezeCapability<Fe>,
        mint: coin::MintCapability<Fe>,
    }

    public entry fun init_fe(owner: &signer) {
        assert!(address_of(owner) == @ferum, ERR_NOT_ALLOWED);
        let (burn, freeze, mint) = coin::initialize<Fe>(owner, string::utf8(b"Ferum Token"),string::utf8(b"Fe"), 8, false);
        move_to(owner, FeCapabilities {
            burn,
            freeze,
            mint,
        })
    }
}