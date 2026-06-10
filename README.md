# eventsourcingdb-es

[![Crates.io Version](https://img.shields.io/crates/v/eventsourcingdb-es)](https://crates.io/crates/eventsourcingdb-es)
[![codecov](https://codecov.io/gh/impierce/eventsourcingdb-es/graph/badge.svg?token=69OQPQVDM4)](https://codecov.io/gh/impierce/eventsourcingdb-es)

An [EventSourcingDB](https://docs.eventsourcingdb.io) implementation of the `PersistedEventRepository` trait in [cqrs-es](https://crates.io/crates/cqrs-es).

---

## Usage

Add the following to your `Cargo.toml`:

```toml
[dependencies]
cqrs-es = "0.5.0"
eventsourcingdb-es = "0.1.0"
```

---

## Features implemented

- [x] Persist events
- [x] Read events by subject (aggregate type and ID)
- [x] Read last events by subject
- [x] Stream events for subject
- [x] Stream all events for aggregate type
- [ ] Write snapshots
- [ ] Read snapshots
