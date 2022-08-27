export function addCoinAddressIfNecessary(address: string, coinName: string): string {
  if (coinName.startsWith(address)) {
    return coinName
  }
  return `${address}::${coinName}`
}