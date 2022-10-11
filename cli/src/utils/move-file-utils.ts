import path from 'path';
import { execSync } from "child_process";
import { Env } from "../config";

export type RestoreFn = () => void;

export function updateMoveTOMLForDeploy(env: Env, moduleDir: string): RestoreFn {
  const moveFilePath = path.join(moduleDir, 'Move.toml');

  execSync(`cp ${moveFilePath} ${moveFilePath}.backup`);
  execSync(`sed -i "" '/ferum *= */s/.*//' ${moveFilePath}`);
  execSync(`sed -i "" 's/rev *= *".*"/rev="${env}"/' ${moveFilePath}`);
  return function() {
    execSync(`cp ${moveFilePath}.backup ${moveFilePath}`);
    execSync(`rm ${moveFilePath}.backup`);
  }
}