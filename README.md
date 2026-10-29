# NeuralMimicry Cora

Permissioned organisational memory, effective decisions and sourced briefings.

Cora stores typed facts, decisions, obligations, relationships, proposals, predictions and outcomes. Context is immutable and carries source/object/version, effective time, server recorded time, actor and correction links. Relay is authoritative for source permissions. Retrieval checks live permissions even when asking a historical question; unavailable sources are omitted. The query uses a fixed recorded-before snapshot and selects current effective records across the complete database before paging. Equal-effective conflicting claims remain visible.

Ingest organisational context under a control grant with POST /v1/context. Import Relay events with POST /v1/import/relay and an after cursor; data.context supplies the ContextInput fields except source, which comes from the event. Persist the returned cursor only after success. An interrupted request can repeat durable IDs. A tombstone removes Cora's records for that source object. Refiner's POST /api/assistant/rag-mcp supplies briefings; it retains assistant, Gail and Aria ownership. Generated summaries are returned with approved_decision=false and training_permitted=false and cannot be ingested as a generated-summary record kind.

POST /v1/query accepts text, effective_at, recorded_before and after. Continue next_cursor until null. A denied or unavailable source yields no content; absence is not proof that no source exists. POST /v1/briefings accepts query plus question. Use the query API to export the exact permitted context records. Raw Refiner conversation retention and external source deletion remain with their owners; Cora does not claim to erase independently retained copies.

Automatic extraction, fuzzy entity resolution, graph visualisation and NMSTT browser interactions are not implemented in this release. related_ids supports explicit relationships without inventing entity matches.

## Build and run

Rust 1.98.1 or newer, PostgreSQL 16 or newer. Run `cargo fmt --check`, `cargo clippy --all-targets -- -D warnings`, and `cargo test` (add `--workspace` in Verity). Lockfiles are committed. The shared kernel is a pinned Git dependency from neuralmimicry/verity. CI receives a read-only Verity deploy key through VERITY_READ_KEY. Never replace it with an unrestricted personal access token.

Set DATABASE_URL to a dedicated least-privilege product database, CUSTOMERS_URL to Customers, NM_TENANT to the single configured identity realm (default neuralmimicry), and NM_LISTEN (default 0.0.0.0:8080). Service URLs use <SERVICE>_URL; GAIL_TOKEN and CONDUCTOR_TOKEN are scoped upstream credentials. Caller bearer tokens are only forwarded to Customers, Relay, Verity and Refiner as required. TLS certificate verification is enabled and redirects are disabled. PostgreSQL uses an internal network connection; require cluster network isolation or a trusted TLS proxy for databases across trust boundaries.

`cargo run --release` starts the service. GET /healthz checks the process; GET /readyz checks its database. Business APIs require a live Customers observe/use/control grant for this service. Source operations also require Relay observe access and current source-purpose rights. The service never trusts forwarded identity headers or treats service-account tokens as human users. Each deployment is a single tenant realm; use separate databases and deployments for different identity realms.

HTTP bodies are limited to 1 MiB, upstream responses to 2 MiB, concurrent requests to 64, the database pool to eight and CPU evaluations to four. Requests have a 30-second timeout; CPU jobs retain their permits after caller cancellation. Product databases store mutable governed content; do not put their raw payloads in an immutable ledger. Logs avoid tokens and request bodies.

## Operations and acceptance

The deployment owner is SwarmHPC. Use `swarmhpc/ansible/continuum_tenant_cora_site.yml`, or `continuum_tenant_business_suite_site.yml` for all four. See [operations](docs/operations.md) and [QA](docs/qa.md). Production evidence must record source commit, image digest, rollout/readiness, functional checks and limitations. A healthy pod does not qualify a model or prove an external business effect.

