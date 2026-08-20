# One Dockerfile, two targets, one shared builder.
#
#   docker build -f infra/docker/Dockerfile --target api    -t hookrelay-api:dev    .
#   docker build -f infra/docker/Dockerfile --target worker -t hookrelay-worker:dev .
#
# Replaces the earlier pair of separate Dockerfiles. Each of those ran its own
# `mvn dependency:go-offline`, and because the module lists differed (`-pl common,api` versus
# `-pl common,worker`) the layers had different commands and shared no cache — so building both
# images downloaded the entire dependency tree twice. A single builder stage resolves once and both
# runtime stages copy from it.
#
# Build context is the repository root: both services depend on the `common` module.

# ---- shared builder ----------------------------------------------------------------------------
FROM maven:3.9-eclipse-temurin-21 AS builder
WORKDIR /build

# Poms first, then dependencies, then source. Dependencies change rarely and source changes
# constantly, so a code-only change reuses the cached dependency layer instead of re-downloading.
COPY pom.xml .
COPY common/pom.xml common/
COPY api/pom.xml api/
COPY worker/pom.xml worker/

# The cache mount is what makes rebuilds fast. Without it there is no ~/.m2 inside the container, so
# every build downloads the entire dependency tree again — measured at roughly 8 minutes per image on
# this machine. A cache mount persists across builds without becoming a layer in the final image, so
# it costs nothing in image size and does not leak the cache into the artifact.
RUN --mount=type=cache,target=/root/.m2 mvn -B dependency:go-offline -Dmaven.test.skip=true

COPY common/src common/src
COPY api/src api/src
COPY worker/src worker/src
# -Dmaven.test.skip skips COMPILING tests as well as running them. -DskipTests still
# compiles them, which makes the image build depend on the test classpath resolving inside
# the container — an unnecessary failure mode, since tests run in CI, not here.
RUN --mount=type=cache,target=/root/.m2 mvn -B package -Dmaven.test.skip=true

# ---- runtime base ------------------------------------------------------------------------------
# A fresh base, so nothing from the builder — Maven, the JDK, the source, the ~/.m2 cache — is in the
# final image. Deleting them in a later layer would not help: every layer is still shipped.
#
# Alpine JRE rather than distroless: the preStop hook needs a shell to run `sleep`, and being able to
# open a shell in a cluster this project exists to be demonstrated on is worth the larger surface.
FROM eclipse-temurin:21-jre-alpine AS runtime-base

# Container isolation is namespaces and cgroups, not a VM, so a root process that escapes is root on
# the host. Nothing here needs it: ports are above 1024 and nothing is written outside /tmp.
# A NUMERIC uid, not a name. Kubernetes' runAsNonRoot cannot resolve a username to a uid without
# inspecting the image, so a `USER hookrelay` image fails to start with
# "has non-numeric user (hookrelay), cannot verify user is non-root".
RUN addgroup -g 1000 -S hookrelay && adduser -u 1000 -S -G hookrelay hookrelay
USER 1000
WORKDIR /app

# The JVM's default heap fraction assumes the machine is doing other things; in a container it is the
# only occupant. The container's memory limit must still exceed the heap, because a Java process is
# heap PLUS metaspace, thread stacks, code cache and direct buffers — and exceeding a memory limit is
# an OOM kill, not throttling.
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseContainerSupport -XX:+ExitOnOutOfMemoryError"
ENTRYPOINT ["sh", "-c", "exec java $JAVA_OPTS -jar app.jar"]

# ---- api ---------------------------------------------------------------------------------------
FROM runtime-base AS api
COPY --from=builder --chown=hookrelay:hookrelay /build/api/target/hookrelay-api-*.jar app.jar
EXPOSE 8080

# ---- worker ------------------------------------------------------------------------------------
FROM runtime-base AS worker
COPY --from=builder --chown=hookrelay:hookrelay /build/worker/target/hookrelay-worker-*.jar app.jar
# Management endpoints only: Prometheus scrapes here and the Kubernetes probes live here. The worker
# serves no application traffic and has no Service in front of it.
EXPOSE 8081
