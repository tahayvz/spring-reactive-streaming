# Spring Reactive Streaming

[![CI](https://github.com/tahayvz/spring-reactive-streaming/actions/workflows/ci.yml/badge.svg)](https://github.com/tahayvz/spring-reactive-streaming/actions/workflows/ci.yml)
[![Java](https://img.shields.io/badge/Java-21-007396?logo=openjdk&logoColor=white)](https://openjdk.org/projects/jdk/21/)
[![Spring WebFlux](https://img.shields.io/badge/Spring%20WebFlux-3.5-6DB33F?logo=springboot&logoColor=white)](https://spring.io/reactive)
[![R2DBC](https://img.shields.io/badge/R2DBC-PostgreSQL-336791?logo=postgresql&logoColor=white)](https://r2dbc.io/)
[![License](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

Backpressure, stream composition and R2DBC in Spring WebFlux — built around one question:

> **Virtual threads made blocking cheap. So what is reactive still for?**

The answer this project demonstrates is **backpressure**. Virtual threads let you block
without paying for an OS thread. They do not give a slow consumer any way to tell a fast
producer to slow down. Reactive streams carry that signal in the protocol itself.

---

## The part virtual threads don't cover

A producer that outruns its consumer forces a choice, and every option gives something up:

| Strategy | Keeps | Gives up | Fits |
| --- | --- | --- | --- |
| `BUFFER` | everything, up to capacity | fails when capacity is exceeded | data that must not be lost |
| `DROP_LATEST` | the **oldest** values | newer updates | append-only history |
| `DROP_OLDEST` | the **newest** values | older updates | dashboards, live prices |

No library can pick for you: in a price feed the newest value is the only one that
matters, and in a payment feed nothing may be dropped.

Measured, not asserted. A producer pushes 100 values ignoring demand into a buffer of 8;
the consumer subscribes without requesting anything, then asks for 8 once production is
over — which is the only way to see what the buffer actually kept:

```
BUFFER       → 0,1,2,3,4,5,6,7    then OverflowException
DROP_LATEST  → 0,1,2,3,4,5,6,7    then complete
DROP_OLDEST  → 92,93,94,95,96,97,98,99   then complete
```

`BUFFER` is worth a second look: it does not quietly discard anything. It fails loudly,
which is the correct behaviour when silent loss would be worse than an outage.

### The ordering trap

Written with `StepVerifier` the obvious way, all three strategies return the same values
and the test proves nothing. `StepVerifier.create(flux, 0).thenRequest(8)` grants demand
*before* the producer runs, so values pass straight through and the buffer never fills.

The tests here subscribe with no demand and request only after production finishes
(see [`BufferProbe`](src/test/java/com/tahayavuz/reactive/stream/BufferProbe.java)). It
depends on call ordering rather than timing, so it is deterministic — the whole
backpressure suite runs in 10 ms.

---

## Stream composition

| Operator | Why this one |
| --- | --- |
| `merge` | `concat` waits for the first source to finish; price feeds never finish, so the second source would never be heard from |
| `distinctUntilChanged` | Sources report the same price many times a second; sending unchanged data wastes bandwidth and repaints |
| `sample` | Emits the last value per window and discards the rest — a dashboard wants the current price, not every price it missed |

Sampling a 100 ms feed into 1-second windows turns 30 values into 4. That is verified in
**virtual time**: a three-second behaviour is asserted without waiting three seconds, so
the test is fast and does not go flaky on a loaded machine.

---

## R2DBC

`ReactiveCrudRepository` returning `Flux`, so rows stream as they arrive instead of being
collected into a list first. Writes use `concatMap`, not `flatMap` — `flatMap` runs
concurrently and would reorder the history.

Integration tests run against **real PostgreSQL** in Testcontainers. The R2DBC driver is a
separate implementation from JDBC; an embedded database would not prove it works.

---

## API

| Method | Path | Returns |
| --- | --- | --- |
| `GET` | `/api/v1/prices/{symbol}/stream` | Live prices as Server-Sent Events |
| `GET` | `/api/v1/prices/{symbol}/recent?limit=20` | Stored prices, newest first |

The stream endpoint returns `Flux`, so the response stays open and values are pushed as
they are produced. When the client disconnects, Reactor cancels the subscription and the
producer stops — no cleanup code needed.

---

## When not to use this

Reactive is not free. The stack trace stops being a call stack, debugging is harder, and
every developer on the team has to know the operators.

For plain request/response over a database, **Spring MVC with virtual threads is simpler
and performs comparably** — that is exactly what
[java-concurrency-benchmarks](https://github.com/tahayvz/java-concurrency-benchmarks)
measures: on I/O-bound work, virtual threads reach the same scalability without the
programming model.

Reach for reactive when you actually need what it uniquely provides:

- **Backpressure** — a fast producer that must be slowed down or shed load
- **Streaming** — long-lived connections, SSE, WebSocket
- **Composition** — merging, sampling and time-windowing several async sources

---

## Running it

Requires JDK 21 and Docker.

```bash
mvn test
```

```bash
docker run -d --name prices -p 5432:5432 \
  -e POSTGRES_DB=prices -e POSTGRES_USER=prices -e POSTGRES_PASSWORD=prices \
  postgres:16-alpine
```

```bash
mvn spring-boot:run
```

```bash
curl -N http://localhost:8083/api/v1/prices/BTCUSD/stream
```

`-N` disables curl's buffering, so events appear as they arrive.

---

## Tests

27 tests. Only the R2DBC and web suites need Docker.

| Suite | Count | Covers |
| --- | ---: | --- |
| `BackpressureTest` | 6 | Buffer overflow, both drop strategies, zero-demand behaviour |
| `PriceStreamTest` | 6 | merge, deduplication, sampling in virtual time, error propagation |
| `PriceTickStoreTest` | 5 | Real PostgreSQL: persistence, ordering, limits |
| `PriceControllerTest` | 4 | SSE content type and streaming, symbol normalisation |

## License

MIT — see [LICENSE](LICENSE).
