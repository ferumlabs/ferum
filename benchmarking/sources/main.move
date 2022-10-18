module test::benchmarking {

    use std::vector;
    use std::signer::address_of;
    use aptos_std::table;

    struct Holder has key {
        t: table::Table<u64, u8>,
        len: u64,
    }

    public entry fun create_vec(acc: &signer) {
        // Lets create the Holder with an empty vector.
        let t = table::new<u64, u8>();
        move_to(acc, Holder {
            t,
            len: 0,
        })
    }

    public entry fun expand_vec(acc: &signer) acquires Holder {
        // Lets add to the vector to see what happens.
        let h = borrow_global_mut<Holder>(address_of(acc));
        let t = &mut h.t;
        let i = h.len;
        while (i < h.len + 1000) {
            table::add(t, i, 1);
            i = i + 1;
        };
        h.len = i;
    }

    public entry fun add_to_vec(acc: &signer) acquires Holder {
        // Lets add to the vector to see what happens.
        let t = &mut borrow_global_mut<Holder>(address_of(acc)).t;
        table::add(t, 900000, 1);
    }
}