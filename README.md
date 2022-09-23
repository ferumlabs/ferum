![github](https://user-images.githubusercontent.com/111548547/192037827-733a5483-905a-4964-91d1-7e4cbf03fae9.png)

[![Generic badge](https://img.shields.io/badge/ferum-docs-blue.svg)](https://ferum.gitbook.io/ferum-dex/) ![](https://img.shields.io/discord/1014040797487824896?label=discord) ![](https://img.shields.io/badge/liquidity-high-brightgreen)


# Ferum

Ferum is an on-chain order book offering unprecedented control to liquidity providers on [Aptos Ecosystem](https://twitter.com/AptosLabs)! 
For documentation on getting-started, better understanding of the architecture, and partnerships, please refer 
to [Ferum's Official Documentation](https://ferum.gitbook.io/ferum-dex/). 

Documentation below will mostly cover instructions for contributions & pull requests. 

## Quick References

1. A good reference to the Move Language is the [Move Book](https://move-language.github.io/move/introduction.html).
2. The easiest way to publish a module is through the [Aptos CLI](https://aptos.dev/cli-tools/aptos-cli-tool/install-aptos-cli).
3. For developer support, join **🛠 #dev-discussions** group in [discord](http://discord.gg/ferum.).
4. A good read on [building super fast order books](https://gist.github.com/halfelf/db1ae032dc34278968f8bf31ee999a25?permalink_comment_id=3176518).

## Contributing

We welcome all contributions; just make sure that you add unit tests to all new code added, and run `aptos move test` before making a pull request.

## Deployment Instructions

All active development takes place on the main branch. During a release, all main branch commits get rebased on top of devnet branch.
Once rebased, the ferum module either gets published under the same account if backwards compatible, or a new account if not. 

High level instructions for releasing a devnet branch: 

1. Make sure all unit tests are passing! ✅
1. Update your aptos CLI and make sure you have [latest](https://github.com/aptos-labs/aptos-core/releases/); usually breaks if you don't! Run `aptos --version` to find out which one you have and compare to the latest release.
2. `git checkout main; git pull --rebase` to get latest commits on the main branch.
3. `git checkout devnet; git rebase main devnet` to rebase all commits from main to devnet
4. Create a new profile via `ts-node cli/src/index.ts create-profile -n ferum-std-devnet` or use an existing account. 
4. `aptos move publish --private-key 0xPRIVATE_KEY --max-gas 10000 --url https://fullnode.devnet.aptoslabs.com/v1 --included-artifacts none` to publish the module. 
5. `git push` to synchronize remote branch once it's been properly published.
