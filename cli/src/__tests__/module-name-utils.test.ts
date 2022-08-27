import {addCoinAddressIfNecessary} from "../module-name-utils"

describe("testing coin address addition", () => {
  test('should add coin address', () => {
    expect(addCoinAddressIfNecessary("0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f", "test_coin::TestCoin")).toBe("0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f::test_coin::TestCoin");
    expect(addCoinAddressIfNecessary("0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f", "0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f::test_coin::TestCoin")).toBe("0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f::test_coin::TestCoin");
  });
});

