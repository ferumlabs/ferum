import { exec } from "child_process";
import { AptosAccount } from "aptos";

/** Publishes a move module the aptos CLI under the hood */
export function publishModuleUsingCLI(
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
  const addrFlag = `--named-addresses ferum=${accountFrom.address()}`;

  return new Promise((resolve, reject) => {
    exec(
      `aptos move publish ${maxGasFlag} ${pkeyFlag} ${dirFlag} ${urlFlag} ${addrFlag}`, 
      (err, stdout, stderr) => {
        if (stderr) {
          console.warn(stderr);
        }
        if (stdout) {
          console.warn(stdout);
        }

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