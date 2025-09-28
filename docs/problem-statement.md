# backfill: a problem statement

- We have some work to do that is asynchronous, and that work has a couple of priority levels. This is usually only two in practice: get it done fast, and take our time about getting it done.
- Most of the work involves making asynchronous calls to other services, both external and internal.
- We might want to batch up the work sometimes.
- The workers will be a scalable number of Rust processes.
- We sometimes need to insert items at the front of the queue, or at high priority.
- We sometimes know time deadlines for when items must be processed, but not always. Low-priority items might not have such deadlines.
- We definitely want to get each task done, so we want to record that we need to do it and the priority, and we need the full priority queue itself to be durable.
- If trying to do any single task fails, we want to retry, with a backoff so we don't overwhelm whatever resources we're consuming when we do the work. If the failure is not recoverable, we want to record the failure (perhaps in some kind of dead letter mailbox, as is tradition).
- We care about performance and memory both, but are willing to use a little more memory to get performance.
- The queue might be huge at times, such as when we are doing backfill on a new data set, but it'll eventually calm down to a steady rate of incoming work. The system needs to handle both situations well.
- The system will be running several of these work queues at once, with different requirements on each queue (allowed rate of work, backpressure, retry requirements).
- We would prefer not invest in a giant queuing system or job runner; we just want to do some internal tasks from services in a distributed system. We are also aware of the dangers of investing too much time writing our own, so we'd prefer to select good existing Rust libraries to help
- We prefer to use well-tested maintained Rust crates in our dependencies.
- Our async library is `tokio`.
- Redis is a possible backend as well as Postgres.
- We prefer [sqlx](https://lib.rs/crates/sqlx) as our relational database client, but might choose to stay flexible.
- This project must be well-tested.
- We do integration tests with a live postgres running in Docker, using transactions to clean up after groups of tests. We do not mock postgres or redis.
