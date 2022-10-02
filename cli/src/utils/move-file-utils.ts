import path from 'path';
import { execSync } from "child_process";

export type RestoreFn = () => void;

export function replaceFerumAddresses(moduleDir: string): RestoreFn {
  const moveFilePath = path.join(moduleDir, 'Move.toml');

  execSync(`cp ${moveFilePath} ${moveFilePath}.backup`);
  execSync(`sed -i "" '/ferum *= */s/.*//' ${moveFilePath}`);
  return function() {
    execSync(`cp ${moveFilePath}.backup ${moveFilePath}`);
    execSync(`rm ${moveFilePath}.backup`);
  }
}