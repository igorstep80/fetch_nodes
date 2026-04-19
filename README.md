# fetch_nodes

Minimal Storj satellite node fetcher.

This repository extracts the core discovery pattern used by `3bl3gamer/storjnet-info`:

- authenticate to a satellite with a Storj API key
- open a metainfo client
- request a dummy object segment
- read the returned `AddressedOrderLimit` entries
- print node IDs and `ip:port` addresses as JSON

## Requirements

- Go 1.22+
- a valid Storj API key with access to the target satellite

## Usage

Create a local `.env` file:

```bash
cp .env.example .env
```

Then put your key into `.env`:

```bash
STORJ_API_KEY='...'
STORJ_SATELLITE_ADDRESS='us1.storj.io:7777'
STORJ_BUCKET='your-existing-bucket'
STORJ_OBJECT_KEY='probe'
STORJ_DB_PATH='nodes.db'
```

For multiple satellites, add:

```bash
STORJ_SATELLITE_CONFIGS='us1,eu1'
STORJ_SATELLITE_US1_ADDRESS='12EayRS2V1kEsWESU9QMRseFhdxYxKicsiFmxrsLZHeLUtdps3S@us1.storj.io:7777'
STORJ_SATELLITE_US1_API_KEY='...'
STORJ_SATELLITE_EU1_ADDRESS='12L9ZFwhzVpuEKMUNUqkaTLGzwY9G24tbiigLiXpmZWKwmcNDDs@eu1.storj.io:7777'
STORJ_SATELLITE_EU1_API_KEY='...'
```

If `STORJ_SATELLITE_CONFIGS` is set, the fetcher will iterate those named configs in one run and merge duplicate nodes by `node_id`.

The program loads `.env` automatically. You can still override values with real shell environment variables or CLI flags.

Run with the `.env` defaults:

```bash
go run .
```

Serve the API from the same database:

```bash
go run . serve
```

When `serve` starts, it prints:

- `0.0.0.0` when listening on all interfaces
- local URL
- Tailscale URL if `tailscale0` is present

Optional password protection:

```bash
STORJ_API_PASSWORD='change-this'
```

You can also pass:

```bash
go run . serve --password change-this
```

Run continuous updates:

```bash
go run . watch
```

Or override the satellite explicitly:

```bash
go run . --satellite eu1.storj.io:7777
```

Use a full node URL if the satellite is not in Storj's known satellite list:

```bash
go run . --satellite '<nodeid>@example.com:7777'
```

Optional proxy:

```bash
go run . --satellite us1.storj.io:7777 --socks5 127.0.0.1:1080
```

The output is JSON like:

```json
{
  "satellite": "12EayRS2V...@us1.storj.io:7777",
  "fetched_at": "2026-04-18T22:00:00Z",
  "count": 110,
  "nodes": [
    {
      "node_id": "12abc...",
      "address": "203.0.113.10:28967",
      "ip": "203.0.113.10",
      "port": 28967
    }
  ]
}
```

## Database

Each successful run upserts the current node snapshot into a local SQLite database.

Default database file:

```bash
nodes.db
```

Stored fields:

- `node_id`
- `address`
- `ip`
- `port`
- `satellite`
- `bucket`
- `object_key`
- `first_seen_at`
- `last_seen_at`
- `created_at`
- `updated_at`

History table:

- `node_ip_history` tracks IP, address, and port changes over time
- each node has at most one `is_current = 1` history row
- repeated fetches update `last_seen_at`
- IP or port changes close the previous history row and insert a new one

## API

Default listen address:

```bash
127.0.0.1:8080
```

Endpoints:

- `GET /health`
- `GET /stats`
- `GET /neighbors/{ip}`
- `GET /nodes/{node_id}`
- `GET /nodes/{node_id}/history`
- `GET /ip/{ip}`
- `GET /subnet/{subnet}`

Examples:

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/stats
curl http://127.0.0.1:8080/neighbors/79.98.145.186
curl http://127.0.0.1:8080/nodes/<node_id>
curl http://127.0.0.1:8080/nodes/<node_id>/history
curl http://127.0.0.1:8080/ip/79.98.145.186
curl http://127.0.0.1:8080/subnet/79.98.145.0/24
```

With password protection enabled, include either a header:

```bash
curl -H 'X-API-Password: change-this' http://127.0.0.1:8080/health
```

or a query parameter:

```bash
curl 'http://127.0.0.1:8080/health?password=change-this'
```

`/stats` returns:

- total current nodes
- total unique current IPs
- new nodes and new unique IPs for:
  - last hour
  - last 24 hours
  - last week

`/neighbors/{ip}` returns only the `/24` count as plain text.

`/health` also includes the current listen address, local URL, and detected Tailscale URLs.

## Continuous Updates

Default fetch interval:

```bash
10s
```

You can override it with:

```bash
STORJ_UPDATE_EVERY='10s'
```

or:

```bash
go run . watch --every 1m
```

## systemd

Unit files are included in `systemd/`.

Suggested install flow on Ubuntu:

```bash
cd /opt/fetch_nodes
env GOCACHE=/tmp/fetch_nodes-gocache GOMODCACHE=/tmp/fetch_nodes-gomodcache go build -o fetch_nodes .
sudo cp fetch_nodes /usr/local/bin/fetch_nodes
sudo cp systemd/fetch-nodes-watch.service /etc/systemd/system/
sudo cp systemd/fetch-nodes-api.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now fetch-nodes-watch.service
sudo systemctl enable --now fetch-nodes-api.service
```

Useful commands:

```bash
sudo systemctl status fetch-nodes-watch.service
sudo systemctl status fetch-nodes-api.service
journalctl -u fetch-nodes-watch.service -f
journalctl -u fetch-nodes-api.service -f
```

## nginx

A basic reverse proxy config is included in `nginx/fetch-nodes-api.conf`.

Install on Ubuntu:

```bash
sudo apt-get update
sudo apt-get install -y nginx
sudo cp nginx/fetch-nodes-api.conf /etc/nginx/sites-available/fetch-nodes-api.conf
sudo ln -sf /etc/nginx/sites-available/fetch-nodes-api.conf /etc/nginx/sites-enabled/fetch-nodes-api.conf
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t
sudo systemctl restart nginx
sudo systemctl enable nginx
```

This proxies only for Tailscale clients:

```bash
Tailscale IPv4 range: 100.64.0.0/10
Tailscale IPv6 range: fd7a:115c:a1e0::/48
```

This is more robust than binding to one Tailscale IP because it keeps working if the machine's Tailscale address changes. Non-Tailscale clients receive `403 Forbidden`.

Reload after updating:

```bash
sudo nginx -t
sudo systemctl reload nginx
```

## Notes

- This stores the latest known snapshot of each fetched node in SQLite.
- For the later `/24` neighbor service, this fetcher can become the ingestion step for `nodes`, `node_ip_history`, and subnet aggregation.
- `.env` is ignored by git, so the API key stays local.
- The bucket must already exist and be accessible by the API key.
