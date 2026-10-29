# Docker

The build context expects a go-zenon checkout at `../../zenon-network/go-zenon` (see [Dependency on go-zenon](development.md#dependency-on-go-zenon)); `docker-compose.yml` and `docker/compose.test.yml` both pass it in as the `go-zenon` additional build context.

Before the first `docker compose up`, set the submission key the example configuration expects:

```
export EXECUTOR_KEY=bd2b1f4b42334f923064f466ed5590379498585b2dd4e42152db0e5ddbe2023e
```

This is the seed of devnet account index 3 (path `m/44'/73404'/3'` of the devnet mnemonic committed in go-zenon's `docker/devnet/README.md`), the `executorAddress` in `docker/executor.json`. It is public and for the devnet only, never for any other network.

For any real key, read it into the variable without echo (`read -rs EXECUTOR_KEY; export EXECUTOR_KEY`) or use a secret file (`FILE` source). Never put a real seed on a command line, where it lands in shell history and process listings.

`docker-compose.yml` passes it to the container as a Compose secret, a file Compose creates inside the container, rather than a bind mount from the host: on Docker Desktop, a 0400 root-owned key bind-mounted read-only was still readable by a separated worker uid. The bind mount's permission bits did not carry through its VM's file-sharing layer, silently, with no error from the supervisor's own checks. The Compose secret does not go through that layer.

Commands:

```
docker compose up --build            # run the executor and the gateway
docker compose down                  # stop and remove them
docker compose -f docker/compose.test.yml run --build --rm test   # run the test suite as root
```

`docker compose logs executor` shows, in order: the `supervisor: enforcement: ...` line, then repeating `executor: znnd node at http://znnd:35997 is unusable: ...; retrying in <delay>` lines. No worker starts, because this environment runs no znnd.

The elevated privileges above (`privileged: true`, `cgroup: private`) are for this local environment only, to let the supervisor delegate a cgroup and separate worker uids inside the container. To run the executor against a real go-zenon devnet, see [Devnet integration test](devnet-test.md).

The `gateway` service runs the image's `gateway` binary as uid 9999, the image's default user, so the executor service sets `user: "0:0"`. Running the image's `executor` outside Compose needs `--user 0:0` for the same reason, because the supervisor separates the worker's uid only as root. The gateway service serves one `LOCAL` domain from `docker/gateway.json`, which holds no credential. The gateway's data volume `gateway-data` takes the owner and mode of the image's `/var/lib/gateway` (9999:9999, 0700), which the gateway's storage check requires; a bind mount in its place needs the same owner and mode. `executor-data` is mounted read-only. The port is published on `127.0.0.1:35999` only. `simulation.maxConcurrent` is 1, and `simulation.uid` stays unset because off root the gateway ignores it.

The service sets `mem_limit` and `memswap_limit` to 3 GiB, which caps the gateway and its simulators together with no swap. When the cap is reached, the kernel's choice is usually a simulator, which the gateway replaces, but it can be the gateway, which stops the container. `cpus: 2` comes with `GOMAXPROCS` 2 for the gateway process. Each simulator starts with an empty environment and sizes its Go runtime to every core of the Docker VM inside the same 2-CPU quota, so a read-heavy simulation can meet `timeoutMs` sooner than on an unthrottled host. The gateway log keeps at most 3 files of 10 MB. `stop_grace_period: 15s` covers the 10 s request wait and the index close.

`docker compose logs gateway` shows repeated `gateway: znnd node at http://znnd:35997 is unusable: ...` lines here, because this environment runs no znnd and the gateway serves nothing until it answers. With a node, query it like this:

```
curl -s -X POST http://127.0.0.1:35999/ -H 'Content-Type: application/json' \
  -d '{"jsonrpc":"2.0","id":1,"method":"zeno_getHead","params":[1]}'
```
