#!/usr/bin/env bash

ts-node cli/src/index.ts test-ferum -m contract
npm test --prefix cli