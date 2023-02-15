// Module defining the CoinType for ferum's test coins.
module ferum::test_coins {
    use aptos_framework::coin;
    use aptos_framework::coin::{BurnCapability, MintCapability, FreezeCapability};
    use std::string;
    use std::signer::address_of;
    use std::signer;

    // Errors.
    const ERR_NOT_ADMIN: u64 = 1;

    // Used internally for testing.
    struct FakeMoneyA {}
    struct FakeMoneyB {}

    // Used in documentation.
    struct APTF {}
    struct USDF {}

    struct USDFCap has key {
        burn: BurnCapability<USDF>,
        mint: MintCapability<USDF>,
        freeze: FreezeCapability<USDF>,
    }

    struct APTFCap has key {
        burn: BurnCapability<APTF>,
        mint: MintCapability<APTF>,
        freeze: FreezeCapability<APTF>,
    }

    public entry fun create_usdf(owner: &signer) {
        assert!(signer::address_of(owner) == @ferum, ERR_NOT_ADMIN);
        let (
            burn,
            freeze,
            mint
        ) = coin::initialize<USDF>(
            owner,
            string::utf8(b"Ferum USD"),
            string::utf8(b"USDF"),
            8,
            true
        );
        move_to(owner, USDFCap {
            burn,
            freeze,
            mint,
        });
    }

    public entry fun create_aptf(owner: &signer) {
        assert!(signer::address_of(owner) == @ferum, ERR_NOT_ADMIN);
        let (
            burn,
            freeze,
            mint
        ) = coin::initialize<APTF>(
            owner,
            string::utf8(b"Ferum APT"),
            string::utf8(b"APTF"),
            8,
            true
        );
        move_to(owner, APTFCap {
            burn,
            freeze,
            mint,
        });
    }

    public entry fun mint_usdf(dest: &signer, amt: u64) acquires USDFCap {
        let cap = borrow_global_mut<USDFCap>(@ferum);
        let minted = coin::mint(amt, &cap.mint);
        coin::deposit(address_of(dest), minted);
    }

    public fun mint_usdf_to_store(dest: &mut coin::Coin<USDF>, amt: u64) acquires USDFCap {
        let cap = borrow_global_mut<USDFCap>(@ferum);
        let minted = coin::mint(amt, &cap.mint);
        coin::merge(dest, minted);
    }

    public fun burn_usdf(coin: coin::Coin<USDF>) acquires USDFCap {
        let cap = borrow_global_mut<USDFCap>(@ferum);
        coin::burn(coin, &cap.burn);
    }

    public entry fun mint_aptf(dest: &signer, amt: u64) acquires APTFCap {
        let cap = borrow_global_mut<APTFCap>(@ferum);
        let minted = coin::mint(amt, &cap.mint);
        coin::deposit(address_of(dest), minted);
    }

    public fun mint_aptf_to_store(dest: &mut coin::Coin<APTF>, amt: u64) acquires APTFCap {
        let cap = borrow_global_mut<APTFCap>(@ferum);
        let minted = coin::mint(amt, &cap.mint);
        coin::merge(dest, minted);
    }

    public fun burn_aptf(coin: coin::Coin<APTF>) acquires APTFCap {
        let cap = borrow_global_mut<APTFCap>(@ferum);
        coin::burn(coin, &cap.burn);
    }
}