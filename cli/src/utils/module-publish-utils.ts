import { exec, execSync } from "child_process";
import { AptosAccount } from "aptos";
import { updateMoveTOMLForDeploy } from "./move-file-utils";
import { Env } from "../config";

/** Publishes a move module the aptos CLI under the hood */
export function publishModuleUsingCLI(
  env: Env,
  moduleName: string,
  rpcUrl: string,
  accountFrom: AptosAccount,
  moduleDir: string,
  maxGas: number,
): Promise<number> {
  const pkeyHex = accountFrom.toPrivateKeyObject().privateKeyHex;
  const pkeyFlag = `--private-key ${pkeyHex}`;
  const maxGasFlag = `--max-gas ${maxGas}`;
  const dirFlag = `--package-dir ${moduleDir}`;
  const urlFlag = `--url ${rpcUrl}`;
  const addrFlag = `--named-addresses ${moduleName}=${accountFrom.address()}`;
  const artifactsFlag = `--included-artifacts none`;
  const gasUnitPriceFlag = `--gas-unit-price 200`;

  console.log(`aptos move publish --assume-yes ${gasUnitPriceFlag} ${maxGasFlag} ${pkeyFlag} ${dirFlag} ${urlFlag} ${addrFlag} ${artifactsFlag}`);

  const restoreMoveFile = updateMoveTOMLForDeploy(env, moduleName, moduleDir);

  return new Promise((resolve, reject) => {
    exec(
      `aptos move publish --assume-yes ${gasUnitPriceFlag} ${maxGasFlag} ${pkeyFlag} ${dirFlag} ${urlFlag} ${addrFlag} ${artifactsFlag}`,
      (err, stdout, stderr) => {
        if (stderr) {
          console.warn(stderr);
        }
        if (stdout) {
          console.warn(stdout);
        }

        restoreMoveFile();

        if (err) {
          reject(err.code || 1);
        } 
        else {
          resolve(0);
        }
      },
    );
  });
}