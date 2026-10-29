# Devnet integration test

`scripts/devnet-test.sh` runs the executor against a real go-zenon devnet and checks that Settlement accepts its batches and that the executor registers and releases their withdrawals. It needs Docker, Go 1.23 and the `../../zenon-network/go-zenon` checkout on a branch with the devnet build that go-zenon's `docker/devnet/README.md` describes. Run it as a non-root user, one run at a time:

```
./scripts/devnet-test.sh
```

The script wipes its own go-zenon devnet, starts its five znnd nodes (not the explorer) and runs `go test -tags devnet ./internal/devnet/`. The test builds `cmd/executor`, `cmd/worker-wasm` and `cmd/gateway` without the tag, so it runs the binaries a deployment runs, against `http://127.0.0.1:35997`. In order, it:

1. waits for momentum 11, after the devnet genesis switches to libp2p at momentum 10, then, as the devnet's network admin (dev address index 1), registers domain 1 with the devnet build's 30-momentum withdrawal delay and the best-effort data-availability mode, configures ZNN as its asset, registers dev address index 3 as the executor of pillar `dev1`, and fuses QSR so that dev address index 3 has plasma, then sets domain 1's relay pause, so that steps 2 to 4 send no relays;
2. deposits into domain 1, starts the executor, stops it while its first send is unresolved and restarts it, then starts a `LOCAL` gateway on the executor's directory. It checks that the restarted executor follows that send to acceptance without sending again, that Settlement's entry equals the commitment the executor signed, and that the pillar and executor-set reads succeed on the real chain;
3. sets domain 1's submit pause, deposits again, sends a zero-deposit `CallL2` to a target with no contract and sets the domain's status to `PAUSED`, checks that nothing is sent while the pause holds, then clears the pause and checks that the batch is accepted while the domain is still `PAUSED`. It checks the call's receipt by send hash at `pending` (not verified) while the pause holds, then at `latest` (verified);
4. stops the executor, removes domain 1's directory, restarts it and deposits again, then checks that the rebuilt batch records for every batch up to the one this step's deposit closes (batches 0 to 2, or 0 to 3 when the `CallL2` lands in a batch of its own) match Settlement's accepted entries and that Settlement accepts the next batch. It checks the call's receipt at `finalized` across the directory removal and starts a `REMOTE` gateway on the `LOCAL` one;
5. clears the relay pause, then checks that the executor registers each of the three deposits' refunds with exactly one `RelayMessage`, that each registered withdrawal is its batch's refund to the depositor, that `ReleaseWithdrawals` empties domain 1's release queue, that Settlement's payout of each refund to the depositor is in a momentum, that every withdrawals file is removed, and that the executor's account chain holds exactly the sends its submission log records. It ends with the byte comparison of the two gateways' answers and a check that `REMOTE` serves no `pending` batch.

Each gateway starts one simulator. The test calls no `zeno_simulate`. A failed run's directory also holds `gateway-local.log` and `gateway-remote.log`.

The devnet domain runs no contract code, so every state root is the empty-state zero root. The test does not cover a state-root change, an inactive pillar, the global pause, rejection retries, or a restart while a relay is unresolved.

A run takes about 52 minutes (measured: 3109 seconds for the test), most of it waiting for momenta; the first run also builds the devnet image. The script builds and runs that image as `zeno-executor-devnet-znnd:latest` (through `docker/devnet.override.yml`), so it never changes the `go-zenon-devnet:latest` tag of a devnet started with `make devnet-up`. On success the script wipes the devnet. On failure it leaves the devnet running, and the test keeps its directory, whose path it logs, with the executor logs, the configuration and the storage directory holding the submission log. The next run of the script wipes a devnet it left running.

The script runs the devnet as the Docker Compose project `zeno-executor-devnet`, so its wipes never remove the volumes of a devnet started with go-zenon's `make devnet-up`. Both devnets use the same container names and ports, so stop that devnet before running the script. `docker compose -f ../../zenon-network/go-zenon/docker-compose.yml -p zeno-executor-devnet down -v` stops and wipes the script's devnet by hand.

`go test ./...` never runs this test and never needs Docker: every file in `internal/devnet` requires the `devnet` build tag.
