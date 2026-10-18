// Command zeno-exec runs the deploy-time module checks and the wasm
// runtime locally. It writes JSON on stdout and exits 0 on success, 1
// on a reported failure and 2 on a usage error.
package main

const usage = `usage:
  zeno-exec version
  zeno-exec validate [--metadata immutable|owner:<address>] <module.wasm>
  zeno-exec run [--network testnet] [--gas-budget <gas>]
  zeno-exec serve [--listen 127.0.0.1:8547] [--allow-origin <origin>]... [--network testnet]

flags:
  --metadata      validate: the contract metadata to encode and hash, immutable or owner:<address>
  --gas-budget    run: refuse a call or deposit whose plasmaLimit exceeds the gas left; each receipt's gasUsed draws it down (default 0, no limit)
  --network       run, serve: the network name that selects the chain id (default testnet)
  --listen        serve: the loopback address to listen on, host 127.0.0.1 or ::1 (default 127.0.0.1:8547)
  --allow-origin  serve: an extra allowed CORS origin, scheme://host[:port]; repeatable
`
