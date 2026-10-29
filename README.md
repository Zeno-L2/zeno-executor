# zeno-executor

[![CI](https://github.com/Zeno-L2/zeno-executor/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/Zeno-L2/zeno-executor/actions/workflows/ci.yml)

The off-chain executor for Zenon's Phase 1 off-chain execution layer. It is a generic, domain-agnostic runtime that runs a domain's deterministic state-transition function off-consensus and settles commitments on L1 through the Settlement embedded contract.

## Components

- **`executor`**: the supervisor. It follows L1, runs one isolated worker process per domain, submits batches to Settlement, and registers and releases withdrawals.
- **`worker-wasm`**: the WASM domain worker, a deterministic Core 1.0 interpreter with gas metering and a three-import host ABI.
- **`gateway`**: a read-only JSON-RPC server for a domain's batches, state, receipts, events and outbox proofs, with simulation of inputs.
- **`zeno-exec`**: a local developer tool that checks and runs contract modules with no node, executor or chain.

## Specifications

The normative specs live in [zeno-docs](https://github.com/Zeno-L2/zeno-docs): `phase-1/SPEC.md` (the on-chain protocol), `phase-1/EXECUTOR.md` (this executor), `architecture.md` (the design companion), `bridges.md` (building bridges on the two) and `phase-1/CHANGELOG.md` (the revision history). Those copies are canonical. A change lands there first, and this repository builds against the result. On any conflict, zeno-docs governs.

## Requirements

- Go 1.23.
- A checkout of the [`digitalSloth/go-zenon`](https://github.com/digitalSloth/go-zenon) fork at `../../zenon-network/go-zenon`, on the `feature/settlement` branch. See [Dependency on go-zenon](docs/development.md#dependency-on-go-zenon).
- Docker, for the Docker test environment and the devnet integration test.

## Quick start

Check out both repositories side by side:

```
<root>/zeno/zeno-executor
<root>/zenon-network/go-zenon
```

Build, test and run the supervisor (`docker/executor.json` is a complete configuration example):

```
go build ./...
go test ./...
go build -o <dir>/ ./cmd/...
<dir>/executor -config <file>
```

## Documentation

- **[Architecture](docs/architecture.md)**: every package and what it is responsible for.
- **[Running the executor](docs/executor.md)**: the configuration file, the submission key, worker isolation and exit statuses.
- **[Submission and withdrawals](docs/submission.md)**: how batches reach Settlement, and how withdrawals are registered and released.
- **[On-disk layout](docs/storage.md)**: the storage directory, its files and their modes.
- **[Running the gateway](docs/gateway.md)**: the gateway's configuration, its snapshot cache and its boot.
- **[Gateway JSON-RPC API](docs/gateway-api.md)**: every method, its limits, simulation and the raw-file routes.
- **[zeno-exec](docs/zeno-exec.md)**: the local contract tool, its HTTP server and its release.
- **[Docker](docs/docker.md)**: the image, the local compose environment and the Docker test environment.
- **[Devnet integration test](docs/devnet-test.md)**: running the executor against a real go-zenon devnet.
- **[Development](docs/development.md)**: building, CI and the go-zenon dependency.

## Related repositories

- [zeno-docs](https://github.com/Zeno-L2/zeno-docs): specs, tickets and runbooks.
- [zeno-contract-sdk-rs](https://github.com/Zeno-L2/zeno-contract-sdk-rs): the Rust contract SDK and `cargo zeno`.
- [zeno-client-ts](https://github.com/Zeno-L2/zeno-client-ts): the TypeScript client for the gateway.
- [zeno-console](https://github.com/Zeno-L2/zeno-console): the console for exploring and writing to domains.
- [zeno-devnet](https://github.com/Zeno-L2/zeno-devnet): the one-command devnet with every component.
