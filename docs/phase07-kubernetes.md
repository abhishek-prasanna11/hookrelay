# Phase 7 — Containers and Kubernetes

> Written before the code, per BLUEPRINT.md §30. Sections 6–8 are filled in after implementation.

**Goal of this phase:** both services run in Kubernetes as independently scalable Deployments, and a
rolling deployment under load neither drops requests nor loses deliveries.

**Definition-of-Done items this phase closes:**
- API and worker run as separate Kubernetes Deployments.
- Probes and resource limits are configured.
- Graceful shutdown works.
- Rolling deployment is demonstrated.

---

## 1. Concepts

### 1.1 Multi-stage builds, and why the build tools must not ship

A container image is a stack of read-only layers. Everything in any layer is in the final image,
even if a later layer deletes it — so a naive Dockerfile that compiles in place ships Maven, the JDK,
the source, and the entire `~/.m2` cache alongside the application.

A **multi-stage build** compiles in one stage and copies only the artifact into a second:

```text
   stage 1 (builder)          maven + JDK 21 + source + ~/.m2     ~800 MB
        │
        │  COPY --from=builder  app.jar
        ▼
   stage 2 (runtime)          JRE 21 + app.jar                    ~200 MB
```

The second stage starts from a fresh base, so nothing from stage 1 exists in it. Smaller is not just
tidiness: every byte is pulled onto every node on every rollout, and every tool present is attack
surface for anyone who gets code execution.

Layer ordering matters too. Dependencies change rarely and source changes constantly, so
`pom.xml` is copied and dependencies resolved **before** the source is copied — a source-only change
then reuses the cached dependency layer instead of re-downloading.

### 1.2 Running as root is the default and it is wrong

A container's root is (by default) the host's root, mapped through the same kernel. Container
isolation is namespaces and cgroups, not a virtual machine, so a container escape from a root process
is a root escape. Nothing here needs it: the application binds a port above 1024 and writes nothing
outside `/tmp`.

The manifest also declares `runAsNonRoot: true`, so the cluster refuses to start the pod if the image
would run as UID 0 — belt and braces, because the image and the manifest can drift.

### 1.3 The JVM and cgroups

Before container support existed, a JVM inside a 512 MB container read the *host's* memory — say
64 GB — sized its heap at a quarter of that, and was OOM-killed the moment it tried to use it.

Modern JVMs read the cgroup limit instead. But the default heap is still a *fraction* of it, and the
fraction assumes the machine is doing other things. In a container the JVM is the only occupant, so
`-XX:MaxRAMPercentage=75` is set explicitly rather than left at a default chosen for a different
world.

The container limit must exceed the heap, because a Java process is heap **plus** metaspace, thread
stacks, code cache, and direct buffers. Exceeding a memory limit is not throttled — the kernel
**OOM-kills** the process. There is no graceful degradation.

### 1.4 Requests and limits are two different things

| | Meaning |
|---|---|
| **request** | What the scheduler reserves. Determines which node the pod lands on. |
| **limit** | What the kernel enforces at runtime. |

For **CPU** the limit is enforced by CFS throttling: a pod at its limit is *paused* until the next
scheduling period. This shows up as latency spikes with no error, and it is why a CPU limit set too
low is worse than none at all for a latency-sensitive service.

For **memory** the limit is enforced by killing. Memory is incompressible — you cannot give a process
"less memory, slower".

The API here gets a CPU request but a generous limit; the worker is I/O-bound and barely uses CPU at
all, which is the observation phase 8's autoscaling experiment is built on.

### 1.5 Three probes, and the mistake that turns an outage into an outage plus a restart loop

| Probe | On failure | Question |
|---|---|---|
| **liveness** | container is **restarted** | Is this process wedged beyond recovery? |
| **readiness** | pod removed from Service endpoints | Should traffic go here *right now*? |
| **startup** | restarts, but only before it first succeeds | Has it finished booting? |

The classic and costly mistake is pointing liveness at a check that includes dependencies. If
liveness verifies the database, then a database outage restarts every pod — repeatedly — turning a
recoverable dependency failure into a crash loop that also destroys any in-memory state and makes
recovery slower.

**The rule: liveness asks "is this process broken?", readiness asks "can it serve right now?"**
Spring Boot's `/actuator/health/liveness` and `/actuator/health/readiness` split along exactly this
line, which is why probes are pointed at those rather than at `/actuator/health`.

A **startup probe** exists because a JVM takes seconds to boot. Without one, either the liveness
probe needs an `initialDelaySeconds` long enough for the worst case — delaying detection of a real
hang forever after — or slow starts get killed. The startup probe decouples the two.

### 1.6 Why a rolling update drops requests, and what `preStop` is for

The intent is obvious: bring up new pods, take down old ones, never drop a request. What actually
happens on termination is two *independent, concurrent* sequences:

```text
   kubelet                                  control plane
     │                                        │
     ├─ runs preStop hook                     ├─ pod marked Terminating
     ├─ sends SIGTERM ──────────────►         ├─ endpoints controller removes it
     │                                        ├─ each node's kube-proxy updates iptables
     ├─ waits terminationGracePeriodSeconds   │        ↑
     └─ SIGKILL                               │   takes time, and nobody waits for it
```

**Nothing synchronises these.** The application can receive SIGTERM and begin shutting down while
kube-proxy on some node still has an iptables rule pointing at it — so new connections keep arriving
at a process that has already decided to stop. That is the dropped request, and it is not a bug in
the application.

A `preStop` sleep is the standard fix, and it is honestly a workaround: it delays SIGTERM long enough
for endpoint removal to propagate, while the pod keeps serving normally. Combined with Spring Boot's
`server.shutdown: graceful` — which stops accepting new connections but finishes in-flight requests —
and a `terminationGracePeriodSeconds` comfortably larger than `preStop` + in-flight time, the window
closes.

### 1.7 The worker's shutdown is a different problem, already solved

The worker serves no traffic, so endpoint propagation is irrelevant. Its risk is a delivery that is
in flight when the process dies.

That is already safe by construction from phase 3: the broker holds each message unacknowledged until
the worker acks, so a killed worker's messages are redelivered. Phase 3 measured **zero deliveries
lost** across both crash windows. Graceful shutdown does not make it *correct* — it makes it *tidy*,
by letting in-flight deliveries finish rather than redelivering them and doing the work twice.

Spring AMQP stops its listener container on shutdown; `spring.lifecycle.timeout-per-shutdown-phase`
bounds how long it waits, and `terminationGracePeriodSeconds` must exceed it or the kernel kills the
process mid-delivery anyway.

### 1.8 ConfigMaps and Secrets

A ConfigMap is non-sensitive configuration; a Secret is the same mechanism with a different name and
**base64 encoding, which is not encryption**. Anyone who can read the Secret can read the value, and
encryption at rest is a cluster-level setting that has to be enabled deliberately.

They are still worth using correctly: they keep credentials out of the image and out of git, they can
be rotated without a rebuild, and they scope access through RBAC. BLUEPRINT.md §19 is explicit that
secrets never go in source, the Dockerfile, git, a ConfigMap, or the image.

---

## 2. The problem this phase solves

1. Build small, non-root, layer-cached images for both services.
2. Run them as separate Deployments with correct probes, requests and limits.
3. Configure graceful shutdown so a rolling update drops nothing.
4. Keep credentials in a Secret and configuration in a ConfigMap.
5. Demonstrate a rolling deployment under load and measure what it costs.

Not in this phase: autoscaling (phase 8), CI/CD (phase 9), Ingress, TLS, or a production-grade
PostgreSQL/RabbitMQ — both run as single in-cluster instances, which is right for a demonstration
cluster and wrong for production.

---

## 3. Design options

### 3.1 Base image

| Option | Trade-off |
|---|---|
| `eclipse-temurin:21-jre` | Full JRE on Ubuntu. Familiar, has a shell for debugging, largest. |
| **`eclipse-temurin:21-jre-alpine`** | Much smaller. musl rather than glibc, which is fine for a plain JVM service. |
| `gcr.io/distroless/java21` | Smallest attack surface — no shell at all. That also means no `kubectl exec` debugging, and a `preStop` sleep needs a binary that does not exist. |

**Chosen: Alpine JRE.** Distroless is the better security answer, but the `preStop` hook in §1.6
needs to run `sleep`, and losing the ability to open a shell in a cluster this project is meant to be
*demonstrated* on is a poor trade.

### 3.2 Where images come from

minikube runs its own Docker daemon. Building directly into it with `eval $(minikube docker-env)`
avoids needing a registry for local work, with `imagePullPolicy: IfNotPresent` so the kubelet does
not try to pull a tag that only exists locally. Phase 9 pushes to GHCR for the real pipeline.

### 3.3 Datastores in-cluster

PostgreSQL and RabbitMQ run as single-replica Deployments with PersistentVolumeClaims. This is
deliberately not production shape — no replication, no backups, no operator — and it is the right
scope for demonstrating the *application's* behaviour under deployment and load.

---

## 4. Chosen design

### 4.1 Images

Multi-stage, dependencies cached before source, non-root user, container-aware JVM flags. One
Dockerfile per service, both building from the repository root because they share the `common`
module.

### 4.2 Manifests

```text
   infra/kubernetes/
     00-namespace.yaml
     10-config.yaml          ConfigMap (non-secret) + Secret (credentials)
     20-postgres.yaml        Deployment + Service + PVC
     21-rabbitmq.yaml        Deployment + Service + PVC
     30-api.yaml             Deployment + Service
     31-worker.yaml          Deployment (no Service — it serves no traffic)
     40-receiver.yaml        the phase 3 Python receiver, for demonstrations
```

### 4.3 Shutdown settings

| Setting | API | Worker | Why |
|---|---|---|---|
| `preStop` sleep | 8s | — | Lets endpoint removal propagate before SIGTERM (§1.6) |
| `server.shutdown` | `graceful` | — | Finish in-flight requests, refuse new |
| `spring.lifecycle.timeout-per-shutdown-phase` | 20s | 30s | Bounds the wait |
| `terminationGracePeriodSeconds` | 45 | 60 | Must exceed preStop + in-flight work |
| `maxUnavailable` | 0 | 1 | The API must never lose capacity mid-rollout |
| `maxSurge` | 1 | 1 | |

### 4.4 Probes

Liveness on `/actuator/health/liveness`, readiness on `/actuator/health/readiness`, startup probe to
cover JVM boot. **Liveness deliberately does not check the database** — §1.5.

---

## 5. Implementation plan

1. `infra/docker/Dockerfile.api` and `Dockerfile.worker`.
2. Manifests as listed in §4.2.
3. `infra/kubernetes/deploy.sh` — build into minikube's daemon, apply, wait for rollout.
4. `infra/kubernetes/smoke.sh` — register an endpoint, publish an event, assert delivery.
5. `chaos/rolling-deploy.sh` — the §6 experiment.
6. Update `REFERENCE.md`, `README.md`, `RESULTS.md`.

---

## 6. Experiment — what does a rolling deployment cost?

**Method.** A load generator pod inside the cluster drives the `api` Service for 70 seconds. Fifteen
seconds in, both Deployments are rolling-restarted underneath it. Every request carries a unique
`Idempotency-Key`, so a failure is a real failure rather than a deduplicated retry. Afterwards the
database is queried for deliveries that never reached a terminal state.

Four arms: `preStop` present or removed, crossed with keep-alive or a fresh connection per request.

### Result

| | with `preStop` | without `preStop` |
|---|---:|---:|
| **Keep-alive** — requests sent | 2 367 | 3 620 |
| **Keep-alive** — requests failed | **0** | **0** |
| **Connection per request** — requests sent | 3 713 | 3 537 |
| **Connection per request** — requests failed | **0** | **0** |
| Deliveries created | 13 237 | |
| Deliveries succeeded | **13 237** | |
| **Deliveries lost** | **0** | |
| Rollout duration | 78–103 s | 84–99 s |

**Zero dropped requests across 13 237 requests and four rolling restarts**, and zero deliveries lost.

### The `preStop` hook was not shown to do anything

This is a null result and it is worth stating plainly rather than dressing up. The hook exists to
cover the endpoint-propagation race in §1.6, and **this experiment did not reproduce that race at
all** — the baseline without the hook was equally clean.

The first run used keep-alive connections, which was a fair criticism of the method: the race
affects *new* connections, since those are the ones that follow a fresh iptables lookup. So the load
generator gained a `--no-keepalive` mode and both arms were re-run with a fresh connection per
request. Still zero.

Why the window stays closed here:

- **Single-node cluster.** There is exactly one kube-proxy and one iptables table to update, so
  endpoint removal propagates in well under a second. The race widens with node count, and minikube
  has the narrowest possible version of it.
- **`maxUnavailable: 0`.** A new pod is Ready before an old one begins terminating, so there is
  always a healthy endpoint to route to.
- **`server.shutdown: graceful`.** In-flight requests finish rather than being cut off, which covers
  connections that were already established when SIGTERM arrived.
- **~50 requests/second.** Few enough new connections land inside a sub-second window to make a hit
  unlikely.

**The hook is kept**, and the honest reason is that the race is real, documented, and widens on
exactly the multi-node clusters this would run on in production — not that it was measured helping
here. It costs about 8 seconds per pod on the rollout, visible in the 78–103 s range above. Anyone
reading this should know the measurement is a null result, not a validation.

**Reproduce:**

```bash
./chaos/rolling-deploy.sh with-prestop
```

```bash
EXTRA_ARGS=',"--no-keepalive"' ./chaos/rolling-deploy.sh without-prestop
```

### Build and image results

| Measurement | Value |
|---|---:|
| `hookrelay-api:dev` | **270 MB** |
| `hookrelay-worker:dev` | **268 MB** |
| Full build + deploy, first run (separate Dockerfiles, no cache) | **~33 min** |
| Full build + deploy, cached (shared builder + BuildKit cache mount) | **60 s** |

---

## 7. Failures

**1. A silent-loss bug, found by the experiment — the most valuable thing in this phase.**

The first run reported 0 failed requests but **3 of 1578 deliveries stranded** in `PENDING`
permanently. The state was unambiguous: `attempt_count = 0` (never claimed), outbox row *published*
(so RabbitMQ had confirmed it), and every queue empty **including unacknowledged**. A message had
been published, consumed, acknowledged, and the delivery had evaporated.

The cause: with 1578 deliveries to one endpoint at `max_concurrency = 5`, deferrals are constant.
`RetryPublisher.defer()` used `convertAndSend` — **fire-and-forget, no confirm** — and
`DeliveryListener` acknowledges the original message the instant that returns. A defer publish lost
during a worker shutdown left the delivery with no queue message, no error, and no way back.

That is a direct violation of BLUEPRINT.md §1: *no silent loss of accepted work*.

What makes it worth recording is **where** the mistake was. The outbox publisher has waited for
broker confirms since phase 2, for exactly this reason. That discipline was simply never carried
across to the defer, retry and dead-letter publishes — the worker's `application.yml` had not even
enabled `publisher-confirm-type`, so confirms were unavailable there. The lesson was learned in one
component and not applied in the one that needed it equally.

Fixed by routing all three through a `publishConfirmed` that waits and **throws** on failure, so the
listener negatively acknowledges and the message is redelivered — trading a possible duplicate for a
guaranteed non-loss, the same trade as phases 2, 3 and 4. Verified: **0 stranded across 5 987
deliveries** afterwards, and pinned by four tests in `PublishFailureTest`.

**2. `runAsNonRoot: true` cannot verify a username.** Every pod failed with
`CreateContainerConfigError`: *"has non-numeric user (hookrelay), cannot verify user is non-root"*.
The kubelet cannot resolve a name to a UID without inspecting the image, so it fails closed. Fixed
with `-u 1000` / `USER 1000` in the image and a matching `runAsUser: 1000` in the manifests, so image
and manifest cannot drift apart.

**3. `deploy.sh` overwrote its own ConfigMap.** It created `receiver-script` from
`tools/webhook_receiver.py`, then applied `40-receiver.yaml`, which re-declared the same ConfigMap
with a placeholder. The receiver crash-looped on `can't open file '/app/webhook_receiver.py'`. The
placeholder is gone and the ConfigMap is now created after the manifest is applied.

**4. A 90-second startup probe budget caused a self-inflicted crash loop.** Six JVMs starting at once
on a contended node took longer than `30 × 3s`, so the kubelet killed each pod mid-boot and it never
got to finish starting — pods cycling with exit 143 and 137, which looks exactly like an application
fault. Raised to `60 × 5s`. A generous startup probe costs nothing, because liveness does not run
until it succeeds.

**5. Two Dockerfiles that shared no cache.** Each ran its own `dependency:go-offline` with a
different `-pl` list, so Docker saw different commands and the full dependency tree downloaded twice
— most of the original ~33 minutes. Replaced with one Dockerfile, two targets, one shared builder,
plus a BuildKit `~/.m2` cache mount. Deploy went to 60 s.

**6. The image build was compiling tests.** `-DskipTests` skips *running* tests but still compiles
them, so the image build depended on the test classpath resolving inside the container — and failed
when it did not. `-Dmaven.test.skip=true` skips both. Tests belong in CI, not in a packaging step.

---

## 8. Lessons learned

**A lesson learned in one component does not propagate by itself.** The outbox pattern in phase 2
exists entirely because a write that is not confirmed may not have happened. Three phases later the
retry publisher made precisely that mistake, in a code path where the consequence was identical and
worse-hidden. Reviewing "where else does this rule apply?" when a pattern is introduced would have
caught it; nothing else did, until a chaos experiment ran.

**Chaos experiments earn their keep by finding bugs, not by confirming beliefs.** The rolling-deploy
experiment failed to demonstrate the thing it was built to demonstrate — and found a real silent-loss
bug on the way. The stranded deliveries were only visible because the script checked the *database*
for unfinished work rather than only counting HTTP failures. An experiment that had asserted "0
requests failed, therefore the rollout was clean" would have passed while losing data.

**Report null results as null results.** `preStop` cannot be claimed to work here: the baseline
without it was equally clean across 13 237 requests, including with connection-per-request traffic
specifically designed to expose the race. Keeping the hook is a judgement about production
multi-node clusters, not a conclusion from this data, and saying so is more useful than a graph
implying it saved requests it never saved.

**"Not ready" and "broken" are different, and Kubernetes will happily conflate them if you let it.**
A too-tight startup probe turned slow JVM boot under contention into a crash loop that looked like an
application defect. The startup probe exists precisely to say "slow, not broken", and being stingy
with it converts a non-problem into an outage.

**A container image is a stack of layers, and cache keys are the whole build economy.** Two
Dockerfiles that looked equivalent shared nothing because one flag differed in a `RUN` line, which
cost roughly 30 minutes per build. Understanding what invalidates a layer is not trivia — it is the
difference between a 60-second deploy and a coffee break.
