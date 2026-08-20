# Phase 9 — CI/CD

> Written before the code, per BLUEPRINT.md §30. Sections 6–8 are filled in after implementation.

**Goal of this phase:** every push runs the full suite against real infrastructure and publishes
versioned images; a bad deployment is caught by the cluster and reversed, with the reversal measured.

**Definition-of-Done items this phase closes:**
- GitHub Actions performs CI/CD.
- Images are published to GHCR.
- Rollback is demonstrated.

---

## 1. Concepts

### 1.1 What CI is actually for

Not "running tests" — that can be done locally. CI exists because **local results are not evidence
about anything but the local machine**. This project has already tripped over that twice: Maven's
default JVM here is 26 while the project targets 21, and the test suite needs a Docker daemon. A
clean run on a machine that has been configured by hand says nothing about a machine that has not.

CI answers a narrower and more useful question: *does this commit build and pass on a machine that
knows nothing about my laptop?*

### 1.2 Build once, promote the same artifact

The tempting pipeline builds an image per environment. It is wrong for a reason worth stating: an
image built twice from the same source is not guaranteed to be the same image. Base tags move,
transitive dependency ranges resolve differently, timestamps differ. "Tested in staging" then refers
to a different artifact than the one in production.

```text
   build once ──► image@sha256:abc ──► test ──► deploy staging ──► deploy prod
                       ▲                              ▲                  ▲
                       └──────── the same bytes throughout ──────────────┘
```

The corollary is that **tags must be immutable**. `:latest` cannot express "the thing we tested",
because it means something different tomorrow. Every image here is tagged with the commit SHA;
`:latest` is published as a convenience pointer and is never what a deployment references.

### 1.3 Testcontainers in CI

The suite starts real PostgreSQL and RabbitMQ containers. That works on GitHub-hosted runners
because they ship a Docker daemon — the same reason it works locally — and it is why the tests are
worth running in CI at all: a suite that swaps in an in-memory database in CI is testing a different
system than the one that ships.

The cost is honest: container startup dominates a short suite. Layer caching helps the image build,
not the test run.

### 1.4 Publishing to GHCR

GitHub's registry authenticates with the `GITHUB_TOKEN` that Actions already injects, so no secret
needs creating or rotating. The workflow needs `packages: write` permission, which is not granted by
default — the default token is read-only for packages, a sensible default that produces a confusing
403 the first time.

Images from a private repository are private by default, which is correct here.

### 1.5 Why CD stops at the registry in this project

The pipeline can build, test and publish from a GitHub-hosted runner. It **cannot deploy to the
cluster used in this project**, and pretending otherwise would be dishonest: minikube runs on this
laptop, behind NAT, unreachable from GitHub's runners. Bridging that needs one of

- a **self-hosted runner** inside the network,
- a cluster with a reachable API server and a kubeconfig in secrets, or
- a pull-based deployer in the cluster (Argo CD, Flux) that watches the registry.

The third is what a real deployment would use, and BLUEPRINT.md §2 lists Argo CD as an explicit
non-goal, so it is out of scope.

So the workflow ends at "image published", and deployment is a documented local script. The
**rollback demonstration is the part that matters** and it runs for real against the cluster — a
deliberately broken image, the readiness probe refusing it, and `kubectl rollout undo`.

### 1.6 Why a bad deploy is survivable, and what actually catches it

The mechanism is already in place from phase 7, and it is worth being precise about which piece does
the work:

```text
   deploy a broken image
        │
        ├─ new pod starts, fails its readiness probe
        ├─ never joins the Service's endpoints        ← no traffic ever reaches it
        ├─ maxUnavailable: 0 → no old pod is removed  ← capacity is never reduced
        └─ rollout STALLS rather than completing
```

**The readiness probe is what makes this safe**, not the rollback. Traffic never reaches the broken
pod, and because `maxUnavailable: 0` forbids removing a healthy pod before a new one is Ready, the
old version keeps serving at full capacity. The deployment simply does not finish.

`kubectl rollout undo` is then cleanup, not rescue. The number worth measuring is therefore **how
long the cluster serves traffic in the stalled state without dropping requests**, and how long
recovery takes once someone acts.

`kubectl rollout status` exiting non-zero on timeout is what a real pipeline would gate on to trigger
an automatic undo.

---

## 2. The problem this phase solves

1. Run the full suite on a machine that knows nothing about this laptop.
2. Build both images once and publish them to GHCR tagged by commit SHA.
3. Demonstrate that a broken deployment is refused by the cluster rather than serving traffic.
4. Measure detection and recovery.

Not in this phase: deploying to a cluster from GitHub-hosted runners (§1.5), GitOps, multi-environment
promotion, signing.

---

## 3. Design options

### 3.1 One job or several

| Option | Trade-off |
|---|---|
| Single job: test → build → push | Simplest, and the image cannot be published without the tests passing. Serial, so a slow test run delays the build. |
| Separate jobs with `needs:` | Parallelism and clearer failure attribution. Requires passing artifacts between jobs. |

**Chosen: two jobs.** `test` runs the suite; `images` runs only if it passed, builds both targets and
pushes. Splitting makes "tests failed" and "publish failed" distinguishable at a glance, and the gate
is explicit rather than implicit in step ordering.

### 3.2 Tagging

Every image gets `sha-<commit>`, plus `latest` on the default branch as a convenience. Deployments
reference the SHA tag. See §1.2.

---

## 4. Chosen design

### 4.1 Workflow

```text
   push / PR
       │
       ├─ job: test        JDK 21, full suite (Testcontainers)
       │
       └─ job: images      needs: test
             ├─ build --target api    → ghcr.io/<owner>/hookrelay-api:sha-<commit>
             └─ build --target worker → ghcr.io/<owner>/hookrelay-worker:sha-<commit>
```

Pushes to the registry only happen on the default branch; pull requests build without publishing, so
a fork cannot push images.

### 4.2 The rollback demonstration

```text
   1. baseline: healthy deployment, load running
   2. deploy an image that starts but never becomes Ready
   3. observe: rollout stalls, old pods keep serving, requests keep succeeding
   4. kubectl rollout undo
   5. measure: time stalled, requests failed, time to recover
```

The broken image is built from the real one with a configuration that fails the readiness probe —
closer to a real bad deploy (a service that starts but cannot serve) than a container that refuses to
start at all.

---

## 5. Implementation plan

1. `.github/workflows/ci.yml`.
2. `chaos/rollback.sh` — the §4.2 demonstration.
3. Run it; record.
4. Update `REFERENCE.md`, `README.md`, `RESULTS.md`.

---

## 6. Experiment — what does a bad deployment cost?

**Method.** Continuous load against the API. Mid-flight, deploy a version that starts normally but
points its datasource at a host that does not exist, so the process runs and the readiness probe
never passes. Watch the rollout, then `kubectl rollout undo`.

The broken version is deliberately one that **starts but cannot serve** — a crash-looping container
is the easy case; a process that is up and useless is the one that reaches production.

### Result

| Measurement | Value |
|---|---:|
| Requests sent during the bad deployment | **8 183** |
| **Requests failed** | **0** |
| Ingest p50 / p95 / p99 | 51.4 / 299.6 / 682.5 ms |
| `kubectl rollout status` exit code | **1** — did not complete |
| Time the rollout stalled before intervention | **46 s** |
| Ready replicas throughout | **2** — the old version kept serving |
| Broken pods admitted to the Service | **0** |
| Recovery after `rollout undo` | **2 s** |
| Events accepted / deliveries succeeded | 8 183 / **8 183** |

**A broken version was deployed under load and nothing failed.** Not one of 8 183 requests, and every
accepted event was still delivered.

### What actually did the work

Not the rollback. The **readiness probe** never admitted the broken pods to the Service's endpoints,
so no traffic ever reached them; `maxUnavailable: 0` forbade removing a healthy pod before a new one
was Ready, so capacity never dropped. The deployment simply refused to finish, and the cluster sat in
that state indefinitely, serving normally.

`rollout undo` took 2 seconds and was cleanup, not rescue. **A deployment that cannot become Ready is
not an outage — it is a deployment that did not happen**, and that distinction is entirely the
product of two settings.

`kubectl rollout status` exiting **1** is the signal a pipeline gates on: a CD job would treat that
non-zero exit as "roll back automatically", which is why the number worth quoting is the 46 seconds
of safe stall rather than any recovery time.

**Reproduce:**

```bash
./chaos/rollback.sh
```

---

## 7. Failures

**Nothing broke during this phase**, which is worth recording plainly rather than padding. The CI
workflow is straightforward, and the rollback demonstration behaved exactly as the phase 7 shutdown
settings predicted.

The one thing worth flagging is a **limitation rather than a failure**: the pipeline stops at the
registry. GitHub-hosted runners cannot reach a minikube cluster on this laptop, so the deployment
step is a local script rather than a workflow job. Bridging it needs a self-hosted runner, a
reachable API server with a kubeconfig secret, or a pull-based deployer — and BLUEPRINT.md §2 lists
Argo CD as an explicit non-goal. Writing "deploy" into a workflow that could never run would be
worse than admitting the gap.

The CI job itself is genuinely exercised: it runs the full Testcontainers suite and builds both
images on every push.

---

## 8. Lessons learned

**The rollback is the least important part of a rollback story.** The instinct is that safety comes
from being able to undo. It does not — safety came from the broken version never receiving traffic in
the first place. Readiness probes and `maxUnavailable: 0` meant a completely broken deployment cost
zero requests, and the undo was a 2-second tidy-up afterwards. A pipeline with excellent automated
rollback and no readiness gate would have served errors for however long detection took.

**A non-zero exit code is an interface.** `kubectl rollout status` returning 1 on timeout is what
lets a shell script, a CD job, or a human treat "did not become healthy" as a decision point. The
gate does not need a metrics stack or an analysis window — it needs one process to exit non-zero and
another to check.

**Build once, tag immutably.** `:latest` cannot express "the artifact we tested", because it means
something different tomorrow; every deployment here references a commit SHA. This costs nothing at
the time and is unrecoverable later, when "it worked in staging" refers to bytes nobody can identify.

**Say where the pipeline stops.** It would have been easy to add a `deploy` job that never runs, or
to describe the local script as if CI performed it. The honest version — CI publishes, deployment is
local, here is exactly what would be needed to close the gap — is more useful to anyone reading it,
including me in six months.
