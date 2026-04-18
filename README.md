# world-zap

Hanzo World feeds exposed over the ZAP binary protocol, fronted by WebSockets.

- Endpoint: `wss://zap.world.hanzo.ai/zap`
- Auth: IAM bearer token via `Authorization` header or `?token=...` query parameter
- Backend: streams events from `http://world.hanzo.svc/v1/world/events?stream=1`

## Wire format

Every message is a single binary WebSocket frame:

```
+---------+---------+---------+---------+---------+----+----+---------+
|   0x5A  |  0x41   |  0x50   |  0x01   |  TYPE   | LENGTH  | PAYLOAD |
+---------+---------+---------+---------+---------+---------+---------+
    magic "ZAP" + version 0x01           4-byte BE   JSON bytes (<= 4 MiB)
```

Types:

| Code | Name | Direction |
|------|------|-----------|
| 0x01 | INIT | client -> server |
| 0x02 | INIT_ACK | server -> client |
| 0x10 | PUSH | server -> client |
| 0x12 | RESOLVE | server -> client (tool response) |
| 0x20 | LIST_TOOLS | either |
| 0x22 | CALL_TOOL | client -> server |
| 0xF0 | PING | either |
| 0xF1 | PONG | either |
| 0xFE | ERROR | server -> client |

## Topics

| Topic | Required plan | Source |
|-------|--------------|--------|
| `world.events.all` | free | Fused stream of all event layers |
| `world.events.conflicts` | pro | ACLED-style armed conflicts |
| `world.events.earthquakes` | free | USGS seismic events >= M2.5 |
| `world.events.fires` | free | NASA FIRMS active fire detections |
| `world.markets.quotes` | pro | Equity / FX / commodity quotes |
| `world.markets.crypto` | free | Crypto mid-price quotes |
| `world.ships.ais` | team | Global AIS vessel positions |
| `world.aviation.opensky` | pro | OpenSky flight states |
| `world.news.live` | free | Live news headlines |
| `world.weather.alerts` | free | NOAA/NWS severe weather alerts |

## Tools

Invoked via `CALL_TOOL` frames (0x22) with payload `{"id","name","args"}`. Responses arrive as `RESOLVE` (0x12) frames carrying `{"id","error","content"}`.

| Tool | Args | Notes |
|------|------|-------|
| `subscribe` | `{topic}` | Start streaming PUSH frames |
| `unsubscribe` | `{topic}` | Stop streaming |
| `list_topics` | `{}` | Returns catalog + required plan |
| `publish` | `{topic, payload}` | Admin only (service account) |

## Rate limits

Per user, token bucket refilled continuously:

| Plan | Calls/min |
|------|-----------|
| free | 30 |
| pro | 3000 |
| team | 15000 |
| enterprise | 60000 |

## Environment

Injected at runtime from KMS secret `world-secrets`:

| Var | Default | Purpose |
|-----|---------|---------|
| `ADDR` | `:9999` | HTTP listen address |
| `WORLD_BACKEND` | `http://world.hanzo.svc` | worldmonitor base URL |
| `WORLD_SERVICE_TOKEN` | empty | Service-to-service auth for ingest |
| `IAM_ENDPOINT` | `https://hanzo.id` | IAM OIDC issuer |
| `ADMIN_ORGS` | `hanzo` | CSV list of orgs whose members can publish |
| `AUTH_CACHE_TTL` | `5m` | Token cache TTL (Go duration or seconds) |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | unset | OTLP HTTP collector |
| `OTEL_SERVICE_NAME` | `world-zap` | Resource service name |
| `LOG_LEVEL` | `info` | `debug`/`info`/`warn`/`error` |

## Health

- `GET /healthz` — liveness
- `GET /readyz` — readiness (probes backend `/healthz`)
- `GET /metrics/topics` — per-topic subscriber counts (JSON)

## Clients

### wscat smoke test

```bash
# 1. Connect (token in query so wscat's simple auth works)
wscat -c "wss://zap.world.hanzo.ai/zap?token=$HANZO_WORLD_TOKEN" --binary

# 2. Send LIST_TOOLS (0x20) with empty JSON payload {}
# Frame = 5A 41 50 01 20 00 00 00 02 7B 7D
# In wscat, paste as a binary hex: 5A41 5001 2000 0000 027B 7D

# 3. Subscribe to earthquakes (CALL_TOOL 0x22 with JSON args)
# {"id":"1","name":"subscribe","args":{"topic":"world.events.earthquakes"}}
```

### Node / TypeScript client

```typescript
import WebSocket from "ws";

const MAGIC = Buffer.from([0x5A, 0x41, 0x50, 0x01]);
const CALL_TOOL = 0x22;
const PUSH      = 0x10;
const RESOLVE   = 0x12;

function frame(type: number, obj: unknown): Buffer {
  const payload = obj === undefined ? Buffer.alloc(0) : Buffer.from(JSON.stringify(obj));
  const hdr = Buffer.alloc(9);
  MAGIC.copy(hdr, 0);
  hdr[4] = type;
  hdr.writeUInt32BE(payload.length, 5);
  return Buffer.concat([hdr, payload]);
}

function decode(b: Buffer): { type: number; payload: unknown } | null {
  if (b.length < 9 || b[0] !== 0x5A) return null;
  const len = b.readUInt32BE(5);
  const body = b.subarray(9, 9 + len);
  return { type: b[4], payload: body.length ? JSON.parse(body.toString("utf8")) : null };
}

const ws = new WebSocket(`wss://zap.world.hanzo.ai/zap?token=${process.env.HANZO_WORLD_TOKEN}`);
ws.on("open", () => {
  ws.send(frame(CALL_TOOL, { id: "1", name: "subscribe", args: { topic: "world.events.earthquakes" } }));
});
ws.on("message", (data: Buffer) => {
  const f = decode(data);
  if (!f) return;
  if (f.type === PUSH) console.log("push", f.payload);
  else if (f.type === RESOLVE) console.log("resolve", f.payload);
});
```

## Image

`ghcr.io/hanzoai/world-zap:v0.1.0` (multi-arch: amd64 + arm64)

## Build & test

```bash
go build ./...
go test ./...
./world-zap   # needs WORLD_BACKEND + IAM_ENDPOINT to reach anything useful
```
