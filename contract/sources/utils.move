module ferum::utils {
    const ERR_EXCEED_MAX_EXP: u64 = 1;
    const ERR_FP_PRECISION_LOSS: u64 = 2;
    const ERR_FP_EXCEED_DECIMALS: u64 = 3;

    const FP_NO_PRECISION_LOSS: u8 = 1;
    const FP_ROUND_UP: u8 = 2;
    const FP_TRUNC: u8 = 3;

    const MAX_U64: u64 = 18446744073709551615;
    const DECIMAL_PLACES: u8 = 10;
    const DECIMAL_PLACES_EXP_U128: u128 = 10000000000;
    const DECIMAL_PLACES_EXP_U64: u64 = 10000000000;

    const F0 : u128 = 1;
    const F1 : u128 = 10;
    const F2 : u128 = 100;
    const F3 : u128 = 1000;
    const F4 : u128 = 10000;
    const F5 : u128 = 100000;
    const F6 : u128 = 1000000;
    const F7 : u128 = 10000000;
    const F8 : u128 = 100000000;
    const F9 : u128 = 1000000000;
    const F10: u128 = 10000000000;
    const F11: u128 = 100000000000;
    const F12: u128 = 1000000000000;
    const F13: u128 = 10000000000000;
    const F14: u128 = 100000000000000;
    const F15: u128 = 1000000000000000;
    const F16: u128 = 10000000000000000;
    const F17: u128 = 100000000000000000;
    const F18: u128 = 1000000000000000000;
    const F19: u128 = 10000000000000000000;
    const F20: u128 = 100000000000000000000;

    // Programatic way to get a power of 10.
    public fun exp128(e: u8): u128 {
        if (e == 0) {
            F0
        } else if (e == 1) {
            F1
        } else if (e == 2) {
            F2
        } else if (e == 3) {
            F3
        } else if (e == 4) {
            F4
        } else if (e == 5) {
            F5
        } else if (e == 5) {
            F5
        } else if (e == 6) {
            F6
        } else if (e == 7) {
            F7
        } else if (e == 8) {
            F8
        } else if (e == 9) {
            F9
        } else if (e == 10) {
            F10
        } else if (e == 11) {
            F11
        } else if (e == 12) {
            F12
        } else if (e == 13) {
            F13
        } else if (e == 14) {
            F14
        } else if (e == 15) {
            F15
        } else if (e == 16) {
            F16
        } else if (e == 17) {
            F17
        } else if (e == 18) {
            F18
        } else if (e == 19) {
            F19
        } else if (e == 20) {
            F20
        } else {
            abort ERR_EXCEED_MAX_EXP
        }
    }

    const F064 : u64 = 1;
    const F164 : u64 = 10;
    const F264 : u64 = 100;
    const F364 : u64 = 1000;
    const F464 : u64 = 10000;
    const F564 : u64 = 100000;
    const F664 : u64 = 1000000;
    const F764 : u64 = 10000000;
    const F864 : u64 = 100000000;
    const F964 : u64 = 1000000000;
    const F1064: u64 = 10000000000;
    const F1164: u64 = 100000000000;
    const F1264: u64 = 1000000000000;
    const F1364: u64 = 10000000000000;
    const F1464: u64 = 100000000000000;
    const F1564: u64 = 1000000000000000;
    const F1664: u64 = 10000000000000000;
    const F1764: u64 = 100000000000000000;
    const F1864: u64 = 1000000000000000000;
    const F1964: u64 = 10000000000000000000;

    // Programatic way to get a power of 10.
    public fun exp64(e: u8): u64 {
        if (e == 0) {
            F064
        } else if (e == 1) {
            F164
        } else if (e == 2) {
            F264
        } else if (e == 3) {
            F364
        } else if (e == 4) {
            F464
        } else if (e == 5) {
            F564
        } else if (e == 5) {
            F564
        } else if (e == 6) {
            F664
        } else if (e == 7) {
            F764
        } else if (e == 8) {
            F864
        } else if (e == 9) {
            F964
        } else if (e == 10) {
            F1064
        } else if (e == 11) {
            F1164
        } else if (e == 12) {
            F1264
        } else if (e == 13) {
            F1364
        } else if (e == 14) {
            F1464
        } else if (e == 15) {
            F1564
        } else if (e == 16) {
            F1664
        } else if (e == 17) {
            F1764
        } else if (e == 18) {
            F1864
        } else if (e == 19) {
            F1964
        } else {
            abort ERR_EXCEED_MAX_EXP
        }
    }

    public fun fp_mul(a: u64, b: u64, mode: u8): u64 {
        let a128 = (a as u128);
        let b128 = (b as u128);
        let amt = (a128 * b128) / DECIMAL_PLACES_EXP_U128;
        if (mode != FP_TRUNC) {
            let precisionLoss = amt * DECIMAL_PLACES_EXP_U128 < a128 * b128;
            if (precisionLoss) {
                if (mode == FP_ROUND_UP) {
                    amt = amt + 1;
                } else if (mode == FP_NO_PRECISION_LOSS) {
                    abort ERR_FP_PRECISION_LOSS
                };
            };
        };
        (amt as u64)
    }

    public fun fp_div(a: u64, b: u64, mode: u8): u64 {
        let a128 = (a as u128);
        let b128 = (b as u128);
        let amt = (a128 * DECIMAL_PLACES_EXP_U128) / b128;
        if (mode != FP_TRUNC) {
            let precisionLoss = amt * b128 < a128 * DECIMAL_PLACES_EXP_U128;
            if (precisionLoss) {
                if (mode == FP_ROUND_UP) {
                    amt = amt + 1;
                } else if (mode == FP_NO_PRECISION_LOSS) {
                    abort ERR_FP_PRECISION_LOSS
                };
            };
        };
        (amt as u64)
    }

    public fun fp_convert(a: u64, decimals: u8, mode: u8): u64 {
        let decimalMultAdj = exp64(DECIMAL_PLACES - decimals);
        let intPart = a / DECIMAL_PLACES_EXP_U64;
        let decimalPart = (a % DECIMAL_PLACES_EXP_U64) / decimalMultAdj;
        let val = intPart * exp64(decimals) + decimalPart;
        if (mode != FP_TRUNC) {
            let precisionLoss =  decimalPart * decimalMultAdj < a % DECIMAL_PLACES_EXP_U64;
            if (precisionLoss) {
                if (mode == FP_ROUND_UP) {
                    val = val + 1;
                } else if (mode == FP_NO_PRECISION_LOSS) {
                    abort ERR_FP_PRECISION_LOSS
                };
            };
        };
        val
    }

    public fun fp_round(a: u64, decimals: u8, mode: u8): u64 {
        assert!(decimals < DECIMAL_PLACES, ERR_FP_PRECISION_LOSS);
        let decimalsExp = exp64(DECIMAL_PLACES - decimals);
        let val = a / decimalsExp * decimalsExp;
        if (mode != FP_TRUNC) {
            let precisionLoss =  val < a;
            if (precisionLoss) {
                if (mode == FP_ROUND_UP) {
                    val = val + decimalsExp;
                } else if (mode == FP_NO_PRECISION_LOSS) {
                    abort ERR_FP_PRECISION_LOSS
                };
            };
        };
        val
    }

    #[test]
    fun test_fp_convert() {
        let converted = fp_convert(51230000000, 4, FP_NO_PRECISION_LOSS);
        assert!(converted == 51230, 0);

        let converted = fp_convert(51230000000, 5, FP_NO_PRECISION_LOSS);
        assert!(converted == 512300, 0);

        let converted = fp_convert(51230000000, 10, FP_NO_PRECISION_LOSS);
        assert!(converted == 51230000000, 0);

        let converted = fp_convert(51230000000, 3, FP_NO_PRECISION_LOSS);
        assert!(converted == 5123, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_FP_PRECISION_LOSS)]
    fun test_fp_convert_precision_loss() {
        fp_convert(51230000000, 2, FP_NO_PRECISION_LOSS);
    }

    #[test]
    fun test_fp_convert_lose_precision_round_up_trunc() {
        let converted = fp_convert(51230000000, 2, FP_ROUND_UP);
        assert!(converted == 513, 0);
        let converted = fp_convert(51230000000, 2, FP_TRUNC);
        assert!(converted == 512, 0);
    }

    #[test]
    fun test_fp_round() {
        let rounded = fp_round(51230000000, 4, FP_NO_PRECISION_LOSS);
        assert!(rounded == 51230000000, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_FP_PRECISION_LOSS)]
    fun test_fp_round_precision_loss() {
        fp_round(51230000000, 2, FP_NO_PRECISION_LOSS);
    }

    #[test]
    fun test_fp_round_lose_precision_round_up_trunc() {
        let rounded = fp_round(51230000000, 2, FP_ROUND_UP);
        assert!(rounded == 51300000000, 0);
        let rounded = fp_round(51230000000, 2, FP_TRUNC);
        assert!(rounded == 51200000000, 0);
    }

    #[test]
    fun test_fp_mul() {
        let product = fp_mul(10560000000000, 20560000000000, FP_NO_PRECISION_LOSS);
        assert!(product == 21711360000000000, 0);
    }

    #[test]
    fun test_fp_mul_with_decimals() {
        let product = fp_mul(10560000000, 2056000000000, FP_NO_PRECISION_LOSS);
        assert!(product == 2171136000000, 0);
    }

    #[test]
    fun test_fp_mul_precision_loss_round_up_trunc() {
        let product = fp_mul(1, 1, FP_TRUNC);
        assert!(product == 0, 0);
        product = fp_mul(1, 1, FP_ROUND_UP);
        assert!(product == 1, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_FP_PRECISION_LOSS)]
    fun test_fp_mul_precision_loss() {
        fp_mul(1, 1, FP_NO_PRECISION_LOSS);
    }

    #[test]
    fun test_fp_div() {
        let q = fp_div(10560000000000, 30000000000, FP_NO_PRECISION_LOSS);
        assert!(q == 3520000000000, 0);
    }

    #[test]
    fun test_fp_div_precision_loss_round_up_trunc() {
        let q = fp_div(1, 3, FP_ROUND_UP);
        assert!(q == 3333333334, 0);
        q = fp_div(1, 3, FP_TRUNC);
        assert!(q == 3333333333, 0);
    }

    #[test]
    #[expected_failure(abort_code=ERR_FP_PRECISION_LOSS)]
    fun test_fp_div_precision_loss() {
        fp_div(1, 3, FP_NO_PRECISION_LOSS);
    }

    #[test]
    #[expected_failure]
    fun test_fp_div_exceed_max() {
        let a = 1;
        let b = MAX_U64;
        fp_div(b, a, FP_TRUNC);
    }
}