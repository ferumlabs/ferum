import { AptosAccount, AptosAccountObject, } from "aptos";
import { assert } from "console";
import fs from "fs";
import log from "loglevel";
import { getFaucetClient } from "./aptos-client";

type Profile = AptosAccountObject;

type Config = {
  TypeAliases: { [key: string]: string },
  Profiles: { [key: string]: Profile },
  CurrentProfile: string | null,
  FerumAddress: string | null,
}

export const CONFIG_PATH = `${process.env.HOME}/.ferum_config`;
let ConfigCache: Config = {
  TypeAliases: {},
  CurrentProfile: null,
  Profiles: {},
  FerumAddress: null,
};

if (!fs.existsSync(CONFIG_PATH)) {
  syncConfig()
} else {
  ConfigCache = JSON.parse(fs.readFileSync(CONFIG_PATH).toString());
}

function syncConfig() {
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(ConfigCache, null, 2));
}

function addAddressIfNecessary(address: string | null, type: string): string {
  if (!address) {
    return type;
  }
  if (type.split('::').length < 3) {
    return `${address}::${type}`
  }
  return type
}

export default {
  setFerumAddress: function (address: string) {
    ConfigCache.FerumAddress = address;
    syncConfig()
  },

  getFerumAddress: function () {
    return ConfigCache.FerumAddress
  },

  getProfileAccount: function (name: string): AptosAccount {
    const profile = ConfigCache.Profiles[name];
    if (!profile) throw new Error(`Profile ${name} not in Profile map.`);
    return AptosAccount.fromAptosAccountObject(profile);
  },

  createNewProfile: async function (name: string) {
    const account = new AptosAccount();
    await getFaucetClient().fundAccount(account.address(), 20000);
    if (name in ConfigCache.Profiles) {
      log.debug(`Overwriting profile ${name}`);
    }
    ConfigCache.Profiles[name] = account.toPrivateKeyObject();
    syncConfig()
  },

  importExistingProfile: async function (name: string, privateKey: string, ferumAccount: string) {
    const privateKeyHex = Uint8Array.from(Buffer.from(privateKey, "hex"));
    const account = new AptosAccount(privateKeyHex)
    if (name in ConfigCache.Profiles) {
      log.debug(`Overwriting profile ${name}`);
    }
    ConfigCache.Profiles[name] = account.toPrivateKeyObject();
    this.setFerumAddress(ferumAccount);
    syncConfig()
  },

  setCurrentProfile: function (name: string) {
    if (!(name in ConfigCache.Profiles)) {
      throw new Error(`${name} not a defined profile`);
    }
    ConfigCache.CurrentProfile = name;
    syncConfig()
  },

  getCurrentProfileName: function (): string | null {
    return ConfigCache.CurrentProfile;
  },

  tryResolveAlias: function (maybeAlias: string): string {
    assert(this.getFerumAddress(), "Ferum address not set!")
    if (maybeAlias in ConfigCache['TypeAliases']) {
      return addAddressIfNecessary(
        ConfigCache.FerumAddress,
        ConfigCache['TypeAliases'][maybeAlias],
      );
    }
    return maybeAlias;
  },

  setAliasForType: function (alias: string, type: string) {
    if (alias in ConfigCache['TypeAliases']) {
      log.debug(`Overwriting alias registered with type ${type}`);
    }
    ConfigCache['TypeAliases'][alias] = type;
    syncConfig()
  },

  clearAlias: function (symbol: string): boolean {
    if (!(symbol in ConfigCache['TypeAliases'])) {
      log.debug(`Symbol ${symbol} not registered`);
      return false;
    }
    delete ConfigCache['TypeAliases'][symbol];
    syncConfig()
    return true;
  },

  getTypeAliasMap: function (): { [key: string]: string } {
    return ConfigCache.TypeAliases;
  },

  _private: {
    addAddressIfNecessary
  }
}