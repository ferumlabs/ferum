import Config from "../config"

describe("testing config", () => {
  test('should add address if necessary', () => {
    expect(Config._private.addAddressIfNecessary("0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f", "test_coin::TestCoin")).toBe("0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f::test_coin::TestCoin");
    expect(Config._private.addAddressIfNecessary("0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f", "0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f::test_coin::TestCoin")).toBe("0xc27207dd9813d91f069ebe109c269f17943e5b94271fef29e2292ab5e2f7706f::test_coin::TestCoin");
  });
});

