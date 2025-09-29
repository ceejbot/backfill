# backfill

[![CI](https://github.com/ceejbot/backfill/workflows/CI/badge.svg)](https://github.com/ceejbot/backfill/actions)
[![Coverage](https://img.shields.io/badge/coverage-51.94%25-orange)](https://github.com/ceejbot/backfill/actions)

A boringly-named priority queue system for doing async work. This library and work process wrap the the [graphile_worker crate](https://lib.rs/crates/graphile_worker) to do things the way I want to do them. It's unlikely you'll want to do things exactly this way, but perhaps you can learn by reading the code, or get a jumpstart by borrowing open-source code, or heck, maybe this will do what you need.

## What it does

[ TODO ]

Look at the `examples/` directory and the readme there for practical usage.

## Dead Letter Queue (DLQ) Setup

The backfill library includes comprehensive Dead Letter Queue functionality for handling jobs that exceed their retry limits.

### Automatic Setup

The library can automatically create the DLQ schema:

```rust
use backfill::BackfillClient;

let client = BackfillClient::new("postgresql://localhost/mydb", "my_schema").await?;
client.init_dlq().await?;  // Creates DLQ table if needed
```

### Manual Migration Setup

For production environments with controlled migrations, use the provided SQL files:

```bash
# Using the default graphile_worker schema
psql -d your_database -f docs/dlq_schema.sql

# Using a custom schema name
sed 's/graphile_worker/your_schema/g' docs/dlq_schema.sql | psql -d your_database
```

See [`docs/DLQ_MIGRATIONS.md`](docs/DLQ_MIGRATIONS.md) for detailed migration instructions and integration with popular migration tools.

## LICENSE

This code is licensed via [the Parity Public License.](https://paritylicense.com) This license requires people who build on top of this source code to share their work with the community, too. This means if you hack on it for work, you have to make your work repo public somehow. Fair's fair. See the license text for details.
