This project implements Fluxon, a lightweight stream processing engine that consumes Postgres CDC events from Kafka and writes to Elasticsearch.

## Development Principles
- After every code change, run build, unit tests, and e2e tests before committing.
- If all checks pass, do `git add`, `git commit`, and `git push`.
- Do not generate super big commit!