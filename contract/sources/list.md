
```javascript
module ferum::list {
    use std::vector;

    public fun insert<T>(arr: &mut vector<T>, target: u64, val: T) {
        let originalLen = vector::length(arr);
        assert!(target <= originalLen, 0);

        vector::push_back(arr, val);

        let valIdx = originalLen;
        let i = target;
        loop {
            vector::swap(arr, i, valIdx);

            i = i + 1;
            if (i >= valIdx) {
                break
            };
        }
    }

    #[test]
    fun test_insert() {
        let arr = vector::empty<u128>();
        vector::push_back(&mut arr, 0);
        vector::push_back(&mut arr, 2);
        vector::push_back(&mut arr, 3);
        vector::push_back(&mut arr, 4);

        insert(&mut arr, 1, 10);
        assert!(*vector::borrow(&arr, 0) == 0, 0);
        assert!(*vector::borrow(&arr, 1) == 10, 0);
        assert!(*vector::borrow(&arr, 2) == 2, 0);
        assert!(*vector::borrow(&arr, 3) == 3, 0);
        assert!(*vector::borrow(&arr, 4) == 4, 0);

        insert(&mut arr, 5, 20);
        assert!(*vector::borrow(&arr, 0) == 0, 0);
        assert!(*vector::borrow(&arr, 1) == 10, 0);
        assert!(*vector::borrow(&arr, 2) == 2, 0);
        assert!(*vector::borrow(&arr, 3) == 3, 0);
        assert!(*vector::borrow(&arr, 4) == 4, 0);
        assert!(*vector::borrow(&arr, 5) == 20, 0);

    }

    #[test]
    fun test_insert_emoty() {
        let arr = vector::empty<u128>();
        insert(&mut arr, 0, 10);
        assert!(*vector::borrow(&arr, 0) == 10, 0);
    }
}
```
