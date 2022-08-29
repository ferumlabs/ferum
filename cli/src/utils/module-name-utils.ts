export function addAddressIfNecessary(address: string, coinName: string): string {
  if (coinName.startsWith(address)) {
    return coinName
  }
  return `${address}::${coinName}`
}