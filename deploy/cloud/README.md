# Cloud deployment (Oracle Cloud Always Free)

Split-host deployment across two `VM.Standard.E2.1.Micro` instances
(1 vCPU, 1 GB RAM each — the only practical free-tier option once
ARM A1 capacity refuses to materialise).

## Layout

| Host              | Private IP     | Role                       | Containers       |
|-------------------|----------------|----------------------------|------------------|
| `macro-data`      | 10.0.0.31      | data layer                 | postgres, redis  |
| `macro-compute`   | 10.0.0.122     | compute layer              | app, worker+beat |

Same VCN (`vcn-polymarket-bot`), same default subnet, both x86 Ubuntu 22.04.
The compute host connects to the data host via the VCN private IP.

## One-time setup

1. **Two micro instances** in the same VCN, both Ubuntu 22.04, both with
   public IP + the project SSH key.
2. **VCN ingress rule**: in `vcn-polymarket-bot` → Default Security List,
   add an ingress rule allowing all protocols from CIDR `10.0.0.0/24`
   (intra-VCN traffic).  Without this, the compute host cannot reach
   postgres / redis on the data host.
3. **Bootstrap** each host (Docker, 2 GB swap, iptables open to VCN):
   ```
   scp /tmp/bootstrap-cloud.sh ubuntu@<host>:/tmp/
   ssh ubuntu@<host> "bash /tmp/bootstrap-cloud.sh"
   ```

## Deploy

### Data host

```
mkdir -p ~/macro-data
scp data/docker-compose.yml ubuntu@<data-ip>:~/macro-data/
scp data/.env              ubuntu@<data-ip>:~/macro-data/
ssh ubuntu@<data-ip> "cd ~/macro-data && sudo docker compose up -d"
```

Then restore the schema + data from a WSL dump:
```
pg_dump -U macro -d macro -Fc -f /tmp/macro.dump
scp /tmp/macro.dump ubuntu@<data-ip>:~/macro-data/
ssh ubuntu@<data-ip> "sudo docker cp ~/macro-data/macro.dump macro-data-postgres-1:/tmp/macro.dump && \
    sudo docker exec macro-data-postgres-1 pg_restore -U macro -d macro --no-owner --no-acl /tmp/macro.dump"
```

### Compute host

```
ssh ubuntu@<compute-ip> "mkdir -p ~/macro/services/app"
rsync -az --exclude='__pycache__' services/app/ ubuntu@<compute-ip>:~/macro/services/app/
scp compute/docker-compose.yml ubuntu@<compute-ip>:~/macro/docker-compose.yml
scp compute/.env              ubuntu@<compute-ip>:~/macro/.env
ssh ubuntu@<compute-ip> "cd ~/macro && sudo docker compose build && sudo docker compose up -d"
```

Build takes ~5 min on 1 vCPU.  The compose embeds celery beat in the
worker via `--beat`, so a single worker process handles both scheduled
tasks and Telegram polling.

## Resource tuning (why these settings)

`data/docker-compose.yml`:
- postgres: `shared_buffers=192MB`, `effective_cache_size=512MB`,
  `max_connections=30`.  Conservative for 1 GB RAM total; leaves room
  for redis (96 MB) + OS + swap headroom.
- redis: `maxmemory=64mb`, `appendonly=no`.  Pure cache, no persistence
  needed since strategy state is recomputable.

`compute/docker-compose.yml`:
- app + worker share the same image (`macro-strategy-app:cloud`).
- `--concurrency=1` because a single backtest run already uses several
  hundred MB; parallel backtests OOM the host.
- `--max-tasks-per-child=50` so the worker recycles after 50 tasks,
  releasing any pandas / numpy heap fragmentation.

## Operational notes

- **Both instances are Always Free** as long as we stay within OCI's
  monthly limits (2 micros + 200 GB block + 10 TB egress).
- **The DB lives on `macro-data` only**.  If we lose that host, we lose
  the cache — but every ingestor is idempotent and indicators are
  recomputable.  ~1 day to backfill from public sources.
- **Telegram polling lives on `macro-compute`** as a celery beat task
  (every 15 s).  If both this and a local WSL worker poll the same bot,
  updates race — turn off one side.
