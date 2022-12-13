// Module defining the CoinType for ferum's test coins.
module ferum::test_coins {
    use aptos_framework::coin;
    use aptos_framework::coin::{BurnCapability, MintCapability, FreezeCapability};
    use std::string;
    use std::signer::address_of;
    use std::signer;

    // Errors.
    const ERR_NOT_ADMIN: u64 = 1;

    // Used internally for unit testing.
    struct FakeMoneyA {}
    struct FakeMoneyB {}

    // Used in documentation and product development.
    struct USDF {}
    struct ETHF {}

    struct USDFCap has key {
        burn: BurnCapability<USDF>,
        mint: MintCapability<USDF>,
        freeze: FreezeCapability<USDF>,
    }

    struct ETHFCap has key {
        burn: BurnCapability<ETHF>,
        mint: MintCapability<ETHF>,
        freeze: FreezeCapability<ETHF>,
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

    public entry fun create_ethf(owner: &signer) {
        assert!(signer::address_of(owner) == @ferum, ERR_NOT_ADMIN);
        let (
            burn,
            freeze,
            mint
        ) = coin::initialize<ETHF>(
            owner,
            string::utf8(b"Ferum ETH"),
            string::utf8(b"ETHF"),
            8,
            true
        );
        move_to(owner, ETHFCap {
            burn,
            freeze,
            mint,
        });
    }

    public entry fun mint_usdf(dest: &signer, amt: u64) acquires USDFCap {
        if (!coin::is_account_registered<USDF>(address_of(dest))) {
            coin::register<USDF>(dest);
        };
        let cap = borrow_global_mut<USDFCap>(@ferum);
        let minted = coin::mint(amt, &cap.mint);
        coin::deposit(address_of(dest), minted);
    }

    public entry fun mint_ethf(dest: &signer, amt: u64) acquires ETHFCap {
        if (!coin::is_account_registered<ETHF>(address_of(dest))) {
            coin::register<ETHF>(dest);
        };
        let cap = borrow_global_mut<ETHFCap>(@ferum);
        let minted = coin::mint(amt, &cap.mint);
        coin::deposit(address_of(dest), minted);
    }
}