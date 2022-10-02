export type Env = 'devnet' | 'testnet';

let ENV: Env = 'devnet';

export function setEnv(env: Env) {
  ENV = env;
}

export function getEnv(): Env {
  return ENV;
}
