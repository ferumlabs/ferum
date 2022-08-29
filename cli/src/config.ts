import { AptosAccount, AptosAccountObject } from "aptos";
import fs from "fs";

import log from "loglevel";
import { faucetClient } from "./aptos-client";

type Profile = AptosAccountObject;

type Config = {
  TypeAliases: {[key: string]: string},
  Profiles: {[key: string]: Profile},
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
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(ConfigCache));
} 
else {
  ConfigCache = JSON.parse(fs.readFileSync(CONFIG_PATH).toString());
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
  setFerumAddress: function(address: string) {
    ConfigCache.FerumAddress = address;
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(ConfigCache));
  },

  getProfileAccount: function(name: string): AptosAccount {
    const profile = ConfigCache.Profiles[name];
    if (!profile) throw new Error(`Profile ${name} not in Profile map.`);
    return AptosAccount.fromAptosAccountObject(profile);
  },

  createNewProfile: async function(name: string) {
    const account = new AptosAccount();
    await faucetClient.fundAccount(account.address(), 20000);

    if (name in ConfigCache.Profiles) {
      log.debug(`Overwriting profile ${name}`);
    }
    ConfigCache.Profiles[name] = account.toPrivateKeyObject();
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(ConfigCache));
  },

  setCurrentProfile: function(name: string) {
    if (!(name in ConfigCache.Profiles)) {
      throw new Error(`${name} not a defined profile`);
    }
    ConfigCache.CurrentProfile = name;
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(ConfigCache));
  },

  getCurrentProfileName: function(): string | null {
    return ConfigCache.CurrentProfile;
  },

  tryResolveAlias: function(maybeAlias: string): string {
    if (maybeAlias in ConfigCache['TypeAliases']) {
      return addAddressIfNecessary(
        ConfigCache.FerumAddress,
        ConfigCache['TypeAliases'][maybeAlias],
      );
    }
    return maybeAlias;
  },

  setAliasForType: function(alias: string, type: string) {
    if (alias in ConfigCache['TypeAliases']) {
      log.debug(`Overwriting alias registered with type ${type}`);
    }
    ConfigCache['TypeAliases'][alias] = type;
    fs.writeFileSync(CONFIG_PATH, JSON.stringify(ConfigCache));
  },

  clearAlias: function(symbol: string): boolean {
    if (!(symbol in ConfigCache['TypeAliases'])) {
      log.debug(`Symbol ${symbol} not registered`);
      return false;
    }
    delete ConfigCache['TypeAliases'][symbol];
    return true;
  },

  getTypeAliasMap: function(): {[key: string]: string} {
    return ConfigCache.TypeAliases;
  }
}