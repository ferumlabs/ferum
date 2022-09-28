// TODO: move to ferum_std.

// Linear time addition, deletion, and removal from a sorted map.
module ferum::sorted_map {
    use aptos_std::table_with_length as table;
    use ferum_std::linked_list::{Self, LinkedList};
    use std::option::{Self};
    #[test_only]
    use std::vector;
    #[test_only]
    use std::string;
    #[test_only]
    use std::string::String;

    const ERR_MAP_NOT_EMPTY: u64 = 1;
    const ERR_MAP_EMPTY: u64 = 2;

    const DIR_INCR: u8 = 1;
    const DIR_DECR: u8 = 2;

    struct SortedMap<phantom V: store + drop> has store {
        values: table::TableWithLength<u128, V>,
        keys: LinkedList<u128>,
        dir: u8,
    }

    struct KeyIterator has store, copy, drop {
        underlying: linked_list::ListPosition<u128>,
    }

    public fun new_incr<V: store + drop>(): SortedMap<V> {
        SortedMap<V> {
            values: table::new(),
            keys: linked_list::new(),
            dir: DIR_INCR,
        }
    }

    public fun new_decr<V: store + drop>(): SortedMap<V> {
        SortedMap<V> {
            values: table::new(),
            keys: linked_list::new(),
            dir: DIR_DECR,
        }
    }

    public fun key_iterator<V: store + drop>(map: &SortedMap<V>): KeyIterator {
        KeyIterator {
            underlying: linked_list::iterator(&map.keys),
        }
    }

    public fun get_next_key<V: store + drop>(map: &SortedMap<V>, it: &mut KeyIterator): u128 {
        linked_list::get_next(&map.keys, &mut it.underlying)
    }

    public fun has_next_key(it: &KeyIterator): bool {
        linked_list::has_next(&it.underlying)
    }

    public fun get_ref<V: store + drop>(map: &SortedMap<V>, key: u128): &V {
        table::borrow(&map.values, key)
    }

    public fun length<V: store + drop>(map: &SortedMap<V>): u128 {
        linked_list::length(&map.keys)
    }

    public fun contains<V: store + drop>(map: &SortedMap<V>, key: u128): bool {
        table::contains(&map.values, key)
    }

    public fun add<V: store + drop>(map: &mut SortedMap<V>, key: u128, value: V) {
        if (table::contains(&map.values, key)) {
            table::remove(&mut map.values, key);
            table::add(&mut map.values, key, value);
            return
        };
        table::add(&mut map.values, key, value);

        // Insert key in the right spot.
        let it = linked_list::iterator(&map.keys);
        let i = 0;
        while (linked_list::has_next(&it)) {
            let existingKey = linked_list::get_next(&map.keys, &mut it);
            if (map.dir == DIR_INCR && existingKey > key) {
                linked_list::insert_at(&mut map.keys, key, i);
                return
            } else if (map.dir == DIR_DECR && existingKey < key) {
                linked_list::insert_at(&mut map.keys, key, i);
                return
            };
            i = i + 1;
        };
        // Insert key at the end.
        linked_list::insert_at(&mut map.keys, key, i);
    }

    public fun remove<V: store + drop>(map: &mut SortedMap<V>, key: u128) {
        if (!table::contains(&map.values, key)) {
            return
        };
        linked_list::remove(&mut map.keys, key);
        table::remove(&mut map.values, key);
    }

    // Finds and returns the previous value based on the provided key. If there is no valid previous value, the
    // provided default is returned.
    public fun find_prev_value_ref<V: store + drop>(map: &SortedMap<V>, key: u128, def: &V): &V {
        let it = linked_list::iterator(&map.keys);

        let prevKey = option::none<u128>();
        while (linked_list::has_next(&it)) {
            let currKey = linked_list::get_next(&map.keys, &mut it);

            if (map.dir == DIR_INCR && currKey > key) {
                if (option::is_some(&prevKey)) {
                    return table::borrow(&map.values, option::extract(&mut prevKey))
                } else {
                    return def
                }
            } else if (map.dir == DIR_DECR && currKey < key) {
                if (option::is_some(&prevKey)) {
                    return table::borrow(&map.values, option::extract(&mut prevKey))
                } else {
                    return def
                }
            };

            prevKey = option::some(currKey);
        };

        table::borrow(&map.values, option::extract(&mut prevKey))
    }

    public fun drop_empty_map<V: store + drop>(map: SortedMap<V>) {
        assert!(linked_list::length(&map.keys) == 0, ERR_MAP_NOT_EMPTY);
        let SortedMap<V> {
            keys,
            values,
            dir: _,
        } = map;
        linked_list::drop(keys);
        table::destroy_empty(values);
    }

    #[test_only]
    public fun empty_and_drop_map<V: store + drop>(map: SortedMap<V>) {
        let keys = linked_list::as_vector(&map.keys);
        let i = 0;
        while (i < vector::length(&keys)) {
            let key = *vector::borrow(&keys, i);
            remove(&mut map, key);
            i = i + 1;
        };
        drop_empty_map(map);
    }

    #[test]
    fun test_sorted_map_incr() {
        let map = new_incr<u128>();
        add(&mut map, 1, 1);
        add(&mut map, 1000, 30);
        add(&mut map, 100, 30);
        add(&mut map, 10, 20);

        remove(&mut map, 1000);
        add(&mut map, 1000, 40);

        assert!(*find_prev_value_ref(&map, 0, &999) == 999, 0);
        assert!(*find_prev_value_ref(&map, 1, &999) == 1, 0);
        assert!(*find_prev_value_ref(&map, 5, &999) == 1, 0);
        assert!(*find_prev_value_ref(&map, 9, &999) == 1, 0);
        assert!(*find_prev_value_ref(&map, 10, &999) == 20, 0);
        assert!(*find_prev_value_ref(&map, 99, &999) == 20, 0);
        assert!(*find_prev_value_ref(&map, 150, &999) == 30, 0);
        assert!(*find_prev_value_ref(&map, 2000, &999) == 40, 0);

        empty_and_drop_map(map);
    }

    #[test]
    fun test_sorted_map_decr() {
        let map = new_decr<u128>();
        add(&mut map, 1, 1);
        add(&mut map, 1000, 40);
        add(&mut map, 100, 30);
        add(&mut map, 10, 20);

        assert!(*find_prev_value_ref(&map, 0, &999) == 1, 0);
        assert!(*find_prev_value_ref(&map, 1, &999) == 1, 0);
        assert!(*find_prev_value_ref(&map, 5, &999) == 20, 0);
        assert!(*find_prev_value_ref(&map, 9, &999) == 20, 0);
        assert!(*find_prev_value_ref(&map, 10, &999) == 20, 0);
        assert!(*find_prev_value_ref(&map, 99, &999) == 30, 0);
        assert!(*find_prev_value_ref(&map, 150, &999) == 40, 0);
        assert!(*find_prev_value_ref(&map, 2000, &999) == 999, 0);

        empty_and_drop_map(map);
    }

    #[test]
    fun test_sorted_map_update() {
        let map = new_incr<u128>();
        add(&mut map, 1, 0);
        add(&mut map, 10, 10);
        add(&mut map, 100, 30);
        add(&mut map, 1000, 40);

        add(&mut map, 10, 20);
        add(&mut map, 1, 1);

        assert!(*find_prev_value_ref(&map, 0, &999) == 999, 0);
        assert!(*find_prev_value_ref(&map, 1, &999) == 1, 0);
        assert!(*find_prev_value_ref(&map, 5, &999) == 1, 0);
        assert!(*find_prev_value_ref(&map, 9, &999) == 1, 0);
        assert!(*find_prev_value_ref(&map, 10, &999) == 20, 0);
        assert!(*find_prev_value_ref(&map, 99, &999) == 20, 0);
        assert!(*find_prev_value_ref(&map, 150, &999) == 30, 0);
        assert!(*find_prev_value_ref(&map, 2000, &999) == 40, 0);

        empty_and_drop_map(map);
    }

    //
    // Test helpers.
    //

    #[test_only]
    public fun assert_keys(map: &SortedMap<u128>, expected: vector<u8>) {
        assert!(keys_as_string(map) == string::utf8(expected), 0);
    }

    #[test_only]
    public fun print_keys(map: &SortedMap<u128>) {
        std::debug::print(&keys_as_string(map));
    }

    #[test_only]
    public fun keys_as_string(map: &SortedMap<u128>): String {
        linked_list::list_as_string(&map.keys)
    }
}
