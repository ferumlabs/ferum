
```javascript
module ferum::user {
    use aptos_framework::coin;
    use std::signer::address_of;

    friend ferum::market;

    struct Balance<phantom T> has key {
        owner: address,

        assets: coin::Coin<T>,
        locked_assets: coin::Coin<T>,
    }

    public fun init_balance<T>(owner: &signer, assets: coin::Coin<T>) {
        let balance = Balance<T> {
            owner: address_of(owner),
            assets,
            locked_assets: coin::zero<T>(),
        };
        move_to(owner, balance);
    }

    public(friend) fun unlock_assets<T>(owner: address, amount: u64) acquires Balance {
        let balance = borrow_global_mut<Balance<T>>(owner);
        let holder = coin::extract(&mut balance.locked_assets, amount);
        coin::merge(&mut balance.assets, holder);
    }

    public(friend) fun lock_assets<T>(owner: address, amount: u64) acquires Balance {
        let balance = borrow_global_mut<Balance<T>>(owner);
        let holder = coin::extract(&mut balance.assets, amount);
        coin::merge(&mut balance.locked_assets, holder);
    }


}
```
