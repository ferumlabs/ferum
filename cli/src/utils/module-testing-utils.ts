import { exec } from "child_process";
import { AptosAccount } from "aptos";
import { updateMoveTOMLForDeploy } from "./move-file-utils";
import { Env } from "../config";

/** Tests a move module the aptos CLI under the hood */
export function testModuleUsingCLI(
  env: Env,
  moduleName: string,
  rpcUrl: string,
  accountFrom: AptosAccount,
  moduleDir: string,
): Promise<number> {
  const dirFlag = `--package-dir ${moduleDir}`;
  const addrFlag = `--named-addresses ${moduleName}=${accountFrom.address()}`;

  const restoreMoveFile = updateMoveTOMLForDeploy(env, moduleName, moduleDir);

  return new Promise((resolve, reject) => {
    exec(
      `aptos move test ${dirFlag} ${addrFlag}`, 
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
        } else {
          resolve(0);
        } 
      },
    );
  });
}