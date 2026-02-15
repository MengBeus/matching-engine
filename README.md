# matching-engine (Week 1 scaffold)

Week 1 baseline for the matching engine project.

## Quick Start

```bash
go mod tidy
go run ./cmd/api
```

Health check:

```bash
curl http://127.0.0.1:8080/health
```

## Common Commands

Run:

```bash
go run ./cmd/api
```

Run with custom address:

```bash
APP_ADDR=:9090 go run ./cmd/api
```

PowerShell:

```powershell
$env:APP_ADDR=":9090"; go run ./cmd/api
```

Test:

```bash
go test ./...
```

Lint (if installed):

```bash
golangci-lint run ./...
```
# Cache refresh
