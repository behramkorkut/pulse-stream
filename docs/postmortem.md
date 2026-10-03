# Post-mortem

What I built, what held up, what I got wrong, and what I would do next. Technical detail lives in
[architecture.md](architecture.md), [benchmarks.md](benchmarks.md), [kubernetes.md](kubernetes.md) and
[resilience.md](resilience.md) (written in French); this page is the short, honest version.

## Outcome

A Go pipeline (collector → Kafka → processor → aggregator → MongoDB) that runs locally, in containers and on
Kubernetes, with metrics, a versioned dashboard, a fixed-rate load generator, a Helm chart, a CI that deploys it on
a throwaway cluster, and a failure test that kills pods with SIGKILL under load.

The property I cared about most is that **an accepted event is counted exactly once**. I verified it end to end
after every load run. In the failure tests it held for loss (no event lost in 9 recorded runs) and did **not** hold
for duplicates (2 runs over-counted, by 0.013 % and 0.0006 %). I did not fix that; I measured it, found the
likely cause and wrote down the fix.

## What worked

- **The end-to-end check.** Comparing MongoDB with what the collector accepted turned "it seems to work" into a
  pass/fail result. Several real problems surfaced through it or through the CI rather than by reading code.
- **Small steps, each verified on the real thing.** Each step ended with commands run on a real cluster or broker,
  not only unit tests.
- **Putting the whole thing in CI**, including a job that deploys on kind and checks counters.
- **Making the test able to fail.** The load generator exits non-zero on a mismatch, so the failure runs could not
  be quietly ignored.

## What I got wrong, and what it taught me

**The lag metric was wrong by about 6×.** I used the consumer library's built-in lag figure. It reads one
partition at a time, and the topics had 6. I noticed because the numbers did not match what `rpk` reported.
I replaced it with a sum over all partitions through the admin API and cross-checked it with `rpk`. Lesson: a
metric you did not verify against a second source is an assumption.

**A flaky test hid a design flaw.** Two integration test packages shared one Redis database and `go test` runs
packages in parallel, so one test's cleanup wiped another's data and produced double counting. The CI caught it;
my laptop never did. Fix: one Redis database per test package.

**I thought `kubectl delete --grace-period=0 --force` simulated a crash. It does not.** The kubelet enforces a
2-second minimum grace period and sends SIGTERM first, which my programs handle cleanly. My first three
failure runs therefore tested a fast graceful stop and passed trivially. I noticed because an expected symptom (a
long stall after the kill) never appeared, and I then checked what the kubelet really does. The real crash test stops
the container through the container runtime with no grace period, and the table of exit codes (137 = SIGKILL) is
part of its output as proof.

**I estimated the risky window at 25 % and it is closer to 2–4 %.** I predicted the aggregator would over-count after
a crash because the counters are written to MongoDB before the event ids are remembered in Redis. Reading the
dashboard afterwards (a MongoDB write takes under 1 ms inside a 15 ms batch) showed the window is much narrower.
I now write the prediction *and the arithmetic behind it* before running an experiment.

**The default timeouts of the Kafka client were a hidden cost.** With a 30-second session timeout, a killed member
stays in the group for 30 seconds: its partitions are unread, survivors cannot commit, their commits exceed the
15-second batch deadline and they exit and restart. Result: up to 175,000 messages of lag and cascading restarts.
Lowering the session timeout to 10 seconds cut the lag about tenfold and removed the cascade.

**That same fix may have made the duplicate problem more visible.** With long stalls, survivors were blocked and
processed fewer batches in parallel. After shortening the stall, two out of three aggregator runs over-counted.
Nine runs cannot confirm or refute this, and I state it as a hypothesis in the resilience document.

**I could not tell two causes apart with one experiment.** An over-count can come from a crash between writing the
counters and remembering the event ids (I reproduced that deterministically in a unit test), or from two instances
processing the same messages during a rebalance, because the duplicate check is not atomic across instances. A
graceful stop cannot trigger the first and still over-counted, which points to the second. I did not observe the
second directly.

**My "idempotent" sessions were only idempotent within one session.** The session store promised that replaying an
event gives the same result, and a test checked it, but only for an event of the current session. Replaying a batch
that crosses a session boundary (e1 at 10:00, e2 at 10:10, e3 at 10:50) reattached e1 and e2 to e3's session, so two
sessions were counted as one. Two paths lead there: a processor crash before the batch is written, and the retry of the
whole batch when a single Redis call fails. A code review found it, not my tests. The load generator stamped every
event with the current time, so no batch ever crossed a boundary, and the end-to-end check does not verify sessions.
The fix stores the result of each event in Redis, with the same lifetime as the visitor's state, and returns it
unchanged on replay. It costs about 200 bytes per human event while the visitor's state lives, so I also raised
Redis's memory limit in the chart. Lesson: test an idempotence claim by replaying sequences, not single calls, and make sure the test data
actually contains the case the claim is about.

**A smaller one.** The Docker builder in my environment did not support BuildKit cache mounts, so a Dockerfile
written for BuildKit failed locally; I removed the mounts so it builds with both builders.

## What I would and would not claim

I would claim: no event lost across all recorded runs; the pipeline recovers from SIGKILL of any consumer;
reliable lag metrics; reproducible measurements (`make` targets, scripts and commands are in the repository).

I would not claim: exactly-once processing; production-scale throughput (one laptop, generator and cluster share the
cores); statistical significance for the failure runs (9 runs, random timing); anything about per-session counters
(not checked by the end-to-end test).

## What I would do next

1. **Idempotent write.** Store the processed Kafka offset (per partition) in the same MongoDB transaction as the
   counters and apply a batch only if its offset is newer. A replayed batch then has no effect, whatever the cause.
   This needs a MongoDB replica set, because transactions do not exist on a standalone instance.
2. **Cooperative rebalancing.** The client I used moves every partition away from every member on each rebalance. A
   client supporting the cooperative protocol would move only what is needed, shrinking the window for the race.
3. **Lag measured outside the consumers**, so the metric survives the death of the consumer it measures. Today the
   aggregator's lag curve disappears exactly when its only pod is down.
4. **More runs per configuration**, verification of the per-session counters, and late or out-of-order events in
   the load generator (they would have exposed the session bug above).
5. **A real readiness check** in the processor and aggregator (Kafka and MongoDB reachable), instead of probing
   `/metrics`.

## What I take away about method

Write the prediction before the measurement. Check that the test tests what you think it tests (the exit code was
the clue for the "crash" that was not one). Make the verification able to fail, then keep the failing runs in the
report. And when a fix works, ask what it exposed.
