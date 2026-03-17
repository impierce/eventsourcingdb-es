# eventsourcingdb-es

[![Crates.io Version](https://img.shields.io/crates/v/eventsourcingdb-es)](https://crates.io/crates/eventsourcingdb-es)

An [EventSourcingDB](https://docs.eventsourcingdb.io) implementation of the `PersistedEventRepository` trait in [cqrs-es](https://crates.io/crates/cqrs-es).

---

## Conventions and assumptions

This adapter follows the naming guidance from the [EventSourcingDB documentation](https://docs.eventsourcingdb.io/) and assumes the following storage conventions:

- Subjects use the form `/<aggregate-type>/<aggregate-id>`, for example `/books/42`.
- Event types in EventSourcingDB use reverse-domain notation, kebab-case names, and an explicit major version suffix, for example `io.eventsourcingdb.library.book-acquired.v1`.
- Event type names are mapped to `cqrs-es` event names by converting between kebab-case and PascalCase.
- Only major event versions are stored in EventSourcingDB (`v1`, `v2`, ...). On the `cqrs-es` side they are exposed as `1.0`, `2.0`, and so on.
- Legacy unversioned event types are still read as version `1.0`, but new writes always use an explicit `.v1` suffix.
- Event payload and metadata are stored in an adapter envelope so `cqrs-es` metadata survives round-trips.

## Usage

Add the following to your `Cargo.toml`:

```toml
[dependencies]
cqrs-es = "0.5"
eventsourcingdb-es = "0.1.0"
```
