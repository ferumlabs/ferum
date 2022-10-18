import { AptosAccount, AptosAccountObject, } from "aptos";
import { assert } from "console";
import fs from "fs";
import log from "loglevel";
import {getClient, getFaucetClient} from "./aptos-client";
import { assertUnreachable } from "./utils/types";

type Profile = AptosAccountObject;

export type Env = 'devnet' | 'testnet' | 'local';

type Config = {
  TypeAliases: { [key: string]: string },
  Profiles: { [key: string]: Profile },
  CurrentProfile: string | null,
  Env: Env,
}

export const CONFIG_PATH = `${process.env.HOME}/.ferum_config`;
let ConfigCache: Config = {
  TypeAliases: {},
  CurrentProfile: null,
  Profiles: {},
  Env: 'testnet',
};

if (!fs.existsSync(CONFIG_PATH)) {
  syncConfig()
} else {
  ConfigCache = JSON.parse(fs.readFileSync(CONFIG_PATH).toString());
}

function syncConfig() {
  fs.rmSync(CONFIG_PATH);
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
  setEnv: function (env: Env) {
    ConfigCache.Env = env;
    syncConfig();
  },

  getEnv: function(): Env {
    return ConfigCache.Env;
  },

  getFerumAddress: function (): string {
    let env = ConfigCache.Env;
    switch (env) {
      case 'devnet':
      case 'testnet':
      case 'local':
        return ConfigCache.Profiles[env].address;
      default:
        assertUnreachable(env);
    }
  },

  getProfileAccount: function (name: string): AptosAccount {
    const profile = ConfigCache.Profiles[name];
    if (!profile) throw new Error(`Profile ${name} not in Profile map.`);
    return AptosAccount.fromAptosAccountObject(profile);
  },

  createNewProfile: async function (name: string) {
    const account = new AptosAccount();
    if (name in ConfigCache.Profiles) {
      log.debug(`Overwriting profile ${name}`);
    }
    ConfigCache.Profiles[name] = account.toPrivateKeyObject();
    syncConfig()
  },

  importExistingProfile: function (name: string, privateKey: string) {
    const privateKeyHex = Uint8Array.from(Buffer.from(privateKey.replace("0x", ""), "hex"));
    const account = new AptosAccount(privateKeyHex)
    if (name in ConfigCache.Profiles) {
      log.debug(`Overwriting profile ${name}`);
    }
    ConfigCache.Profiles[name] = account.toPrivateKeyObject();
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
        this.getFerumAddress(),
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