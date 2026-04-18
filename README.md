# world-gw (repo: hanzoai/world-zap)

**One Go binary. MCP + ZAP native, same process.** Serves two protocols for Hanzo World real-time feeds.

- **ZAP** — binary-framed WebSocket for µs-latency pub/sub at `wss://zap.world.hanzo.ai/zap`
- **MCP** — Model Context Protocol (JSON-RPC 2.0 over Streamable HTTP) at `https://mcp.world.hanzo.ai/mcp`

Both endpoints share auth (IAM bearer token), rate limiter (plan-tier token bucket), tool registry, and the feed ingester. No separate Node sidecar. The image is `ghcr.io/hanzoai/world-zap` but the binary and K8s Deployment are named `world-gw`.

## Endpoints

| Path | Transport | Purpose |
|---|---|---|
| `GET /healthz` | HTTP | Liveness |
| `GET /readyz` | HTTP | Readiness (checks backend reachability) |
| `GET /metrics/topics` | HTTP | Per-topic subscriber + publish counters |
| `GET /` | HTTP | Service metadata (version, tools, topics, endpoints) |
| `/zap` | WebSocket | ZAP session — send INIT, then CALL_TOOL `subscribe` |
| `/mcp` | HTTP POST (JSON-RPC) + optional SSE | MCP initialize / tools/list / tools/call |

## MCP tool catalog (8)

All tools proxy to the worldmonitor backend under `/v1/world/*`.

| Tool | Path |
|---|---|
| `get_events` | `/v1/world/events` |
| `query_conflicts` | `/v1/world/conflicts` |
| `query_infrastructure` | `/v1/world/infra` |
| `track_vessel` | `/v1/world/vessel` |
| `list_live_news` | `/v1/world/news` |
| `get_markets` | `/v1/world/markets` |
| `list_feeds` | `/v1/world/feeds` |
| `ask_analyst` | `/v1/world/analyst` → Zen |

## ZAP topics (10)

`world.events.{all,conflicts,earthquakes,fires}`, `world.markets.{quotes,crypto}`, `world.ships.ais`, `world.aviation.opensky`, `world.news.live`, `world.weather.alerts`.

## Claude Desktop config

```json
{
  "mcpServers": {
    "world": {
      "type": "sse",
      "url": "https://mcp.world.hanzo.ai/mcp",
      "headers": { "Authorization": "Bearer $HANZO_WORLD_TOKEN" }
    }
  }
}
```

## curl smoke test

```bash
# MCP — list tools
curl -sS -X POST https://mcp.world.hanzo.ai/mcp \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}' | jq .

# MCP — call a tool
curl -sS -X POST https://mcp.world.hanzo.ai/mcp \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"list_feeds","arguments":{}}}' | jq .

# ZAP — subscribe to earthquakes
wscat -c "wss://zap.world.hanzo.ai/zap?token=$TOKEN" --binary
# send INIT (0x01), then CALL_TOOL (0x22) subscribe to world.events.earthquakes
```

## Local dev

```bash
go build -o bin/world-gw .
BACKEND_BASE=http://localhost:5173 IAM_ENDPOINT=https://hanzo.id ./bin/world-gw
# Open http://localhost:9999/ for metadata
```

## Deploy

K8s: `infra/k8s/world/gw-deployment.yaml` (single Deployment + Service in `hanzo` namespace, port 9999). Ingress routes both `mcp.world.hanzo.ai` and `zap.world.hanzo.ai` at the same Service.

Image: `ghcr.io/hanzoai/world-zap:v0.1.0` (multi-arch amd64+arm64, distroless).
