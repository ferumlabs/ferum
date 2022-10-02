import { exec } from "child_process";
import { AptosAccount } from "aptos";
import { replaceFerumAddresses } from "./move-file-utils";

/** Tests a move module the aptos CLI under the hood */
export function testModuleUsingCLI(
  rpcUrl: string,
  accountFrom: AptosAccount,
  moduleDir: string,
): Promise<number> {
  const dirFlag = `--package-dir ${moduleDir}`;
  const addrFlag = `--named-addresses ferum=${accountFrom.address()}`;

  const restoreMoveFile = replaceFerumAddresses(moduleDir);

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