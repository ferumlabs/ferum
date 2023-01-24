module ferum::test_utils {
    #[test_only]
    use std::vector;
    #[test_only]
    use aptos_std::table;
    use std::string;

    //
    // Constants.
    //

    const LEFT_BRACKET: u8 = 0x28;
    const RIGHT_BRACKET: u8 = 0x29;
    const LEFT_SQ_BRACKET: u8 = 0x5b;
    const RIGHT_SQ_BRACKET: u8 = 0x5d;
    const COLON: u8 = 0x3a;
    const SPACE: u8 = 0x20;
    const T: u8 = 0x74;
    const F: u8 = 0x66;

    const NINE: u8 = 0x39;
    const EIGHT: u8 = 0x38;
    const SEVEN: u8 = 0x37;
    const SIX: u8 = 0x36;
    const FIVE: u8 = 0x35;
    const FOUR: u8 = 0x34;
    const THREE: u8 = 0x33;
    const TWO: u8 = 0x32;
    const ONE: u8 = 0x31;
    const ZERO: u8 = 0x30;

    //
    // String utils.
    //

    #[test_only]
    public fun charToNum(s: u8): (u64, bool) {
        if (s == NINE) {
            return (9, true)
        };
        if (s == EIGHT) {
            return (8, true)
        };
        if (s == SEVEN) {
            return (7, true)
        };
        if (s == SIX) {
            return (6, true)
        };
        if (s == FIVE) {
            return (5, true)
        };
        if (s == FOUR) {
            return (4, true)
        };
        if (s == THREE) {
            return (3, true)
        };
        if (s == TWO) {
            return (2, true)
        };
        if (s == ONE) {
            return (1, true)
        };
        if (s == ZERO) {
            return (0, true)
        };
        return (0, false)
    }

    #[test_only]
    public fun u64_from_bytes(bytes: &vector<u8>): u64 {
        let i = 0;
        let num = 0u64;
        let size = vector::length(bytes);
        while (i < size) {
            num = num * 10;
            let (val, ok) = charToNum(*vector::borrow(bytes, i));
            assert!(ok, 0);
            num = num + val;
            i = i + 1;
        };
        num
    }

    #[test_only]
    public fun u16_from_bytes(bytes: &vector<u8>): u16 {
        (u64_from_bytes(bytes) as u16)
    }

    #[test_only]
    public fun u64_vector_from_str(str: &string::String): vector<u64> {
        let out = vector::empty<u64>();
        let i = 0;
        let size = string::length(str);
        let bytes = string::bytes(str);
        let holder = vector::empty<u8>();
        let opened = false;
        let closed = false;
        while (i < size) {
            let (_, ok) = charToNum(*vector::borrow(bytes, i));
            if (!ok) {
                if (*vector::borrow(bytes, i) == LEFT_SQ_BRACKET) {
                    opened = true;
                };
                if (*vector::borrow(bytes, i) == RIGHT_SQ_BRACKET) {
                    closed = true;
                };
                if (vector::length(&holder) > 0) {
                    vector::push_back(&mut out, u64_from_bytes(&holder));
                    holder = vector::empty();
                };
                i = i + 1;
                continue
            };
            vector::push_back(&mut holder, *vector::borrow(bytes, i));
            i = i + 1;
        };
        assert!(opened && closed, 0);
        out
    }

    #[test_only]
    public fun u16_vector_from_str(str: &string::String): vector<u16> {
        convert_u64_list_to_u16(&u64_vector_from_str(str))
    }

    #[test_only]
    public fun u64_vector_to_str(elems: &vector<u64>): string::String {
        let str = s(b"[ ");
        let i = 0;
        let size = vector::length(elems);
        while (i < size) {
            string::append(&mut str, u64_to_string(*vector::borrow(elems, i)));
            string::append(&mut str, s(b" "));
            i = i + 1;
        };
        string::append(&mut str, s(b"]"));
        str
    }

    #[test_only]
    public fun u16_vector_to_str(elems: &vector<u16>): string::String {
        u64_vector_to_str(&convert_u16_list_to_u64(elems))
    }

    #[test_only]
    public fun u128_to_string(value: u128): string::String {
        if (value == 0) {
            return string::utf8(b"0")
        };
        let buffer = vector::empty<u8>();
        while (value != 0) {
            vector::push_back(&mut buffer, ((48 + value % 10) as u8));
            value = value / 10;
        };
        vector::reverse(&mut buffer);
        string::utf8(buffer)
    }

    #[test_only]
    public fun u64_to_string(value: u64): string::String {
        u128_to_string((value as u128))
    }

    #[test_only]
    public fun u32_to_string(value: u32): string::String {
        u128_to_string((value as u128))
    }

    #[test_only]
    public fun u16_to_string(value: u16): string::String {
        u128_to_string((value as u128))
    }

    #[test_only]
    public fun bool_to_string(value: bool): string::String {
        if (value) {
            s(b"t")
        } else {
            s(b"f")
        }
    }

    #[test_only]
    public fun s(bytes: vector<u8>): string::String {
        string::utf8(bytes)
    }

    //
    // Vector utils.
    //

    #[test_only]

    public fun assert_vector_contains<T>(vec: &vector<T>, elem: &T) {
        let size = vector::length(vec);
        let i = 0;
        while (i < size) {
            if (vector::borrow(vec, i) == elem) {
                return
            };
            i = i + 1;
        };
        abort 0
    }

    // Generates a list of elements in sequential order.
    #[test_only]
    public fun gen_sequential_list(count: u64, offset: u64): vector<u64> {
        let elems = vector::empty<u64>();
        let i = 0;
        while (i < count) {
            vector::push_back(&mut elems, i+1+offset);
            i = i + 1;
        };
        elems
    }

    // Generates a list of random elements in the given range.
    #[test_only]
    public fun gen_random_list(count: u64, min: u64, max: u64): vector<u64> {
        assert!(min < max, 0);
        assert!(max - min > 5*count, 0); // Want a big enough search space.
        let elems = vector::empty<u64>();
        let i = 0;
        let j = 0;
        let seed = 18446744073709551615u64;
        let seen = table::new();
        while (j < count) {
            let i3 = (i+1) + (i+1) + (i+1);
            let val = (seed % i3 % (max - min)) + min;
            if (table::contains(&seen, val)) {
                i = i + 100;
                continue
            };
            table::add(&mut seen, val, true);
            assert!(val >= min && val <= max, 0);
            vector::push_back(&mut elems, val);
            i = i + 1;
            j = j + 1;
        };
        table::drop_unchecked(seen);
        elems
    }

    #[test_only]
    public fun convert_u64_list_to_u16(list: &vector<u64>): vector<u16> {
        let elems = vector::empty();
        let i = 0;
        let size = vector::length(list);
        while (i < size) {
            vector::push_back(&mut elems, (*vector::borrow(list, i) as u16));
            i = i + 1;
        };
        elems
    }

    #[test_only]
    public fun convert_u16_list_to_u64(list: &vector<u16>): vector<u64> {
        let elems = vector::empty();
        let i = 0;
        let size = vector::length(list);
        while (i < size) {
            vector::push_back(&mut elems, (*vector::borrow(list, i) as u64));
            i = i + 1;
        };
        elems
    }

    // Generates a list of `count` copies of the given element.
    #[test_only]
    public fun gen_list<T: copy + drop>(count: u64, elem: T): vector<T> {
        let elems = vector::empty();
        let i = 0;
        while (i < count) {
            vector::push_back(&mut elems, elem);
            i = i + 1;
        };
        elems
    }

    #[test_only]
    public fun join(strs: vector<string::String>, sep: vector<u8>): string::String {
        let out = s(b"");
        while (!vector::is_empty(&strs)) {
            string::append(&mut out, vector::remove(&mut strs, 0));
            if (vector::length(&strs) > 0) {
                string::append_utf8(&mut out, sep);
            };
        };
        out
    }

    #[test_only]
    public fun assert_vector_equal<T>(output: &vector<T>, expected: &vector<T>) {
        let equal = {
            if (vector::length(output) != vector::length(expected)) {
                false
            } else {
                let equal = true;
                let i = 0;
                while (i < vector::length(output)) {
                    equal = vector::borrow(output, i) == vector::borrow(expected, i);
                    if (!equal) {
                        break
                    };
                    i = i + 1;
                };
                equal
            }
        };
        if (!equal) {
            std::debug::print(&s(b"Vectors not equal"));
            std::debug::print(&s(b"Expected:"));
            std::debug::print(expected);
            std::debug::print(&s(b"Actual:"));
            std::debug::print(output);
            abort 0
        }
    }

    //
    // Fixedpoint utils.
    //

    #[test_only]
    public fun pretty_print_fp(val: u64, decimals: u8): string::String {
        if (val == 0) {
            return s(b"0")
        };
        let digits = vector[];
        while (val != 0) {
            vector::push_back(&mut digits, val % 10);
            val = val / 10;
        };
        vector::reverse(&mut digits);
        let firstNonZero = false;
        let count = 0;
        let out = s(b"");
        while (!vector::is_empty(&digits)) {
            let digit = vector::pop_back(&mut digits);
            count = count + 1;
            if (digit != 0) {
                firstNonZero = true;
            } else if (!firstNonZero && count <= decimals) {
                continue
            };
            string::insert(&mut out, 0, u64_to_string(digit));
            if (count == decimals) {
                string::insert(&mut out, 0, s(b"."));
            };
        };
        if (string::sub_string(&out, 0, 1) == s(b".")) {
            string::insert(&mut out, 0, s(b"0"));
        };
        out
    }
}