# ── Stage 1: build the CL pgloader v3 binary ─────────────────────────────────
# Build context must be the repo root (not clojure/) so the CL source is
# available.  docker-compose files reference this as:
#   build:
#     context: ../../..
#     dockerfile: clojure/tests/Dockerfile
FROM debian:bookworm-slim AS pgloader-v3-builder

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      bzip2 ca-certificates curl freetds-dev gawk git \
      libsqlite3-dev libssl3 libzip-dev make openssl patch \
      sbcl time unzip wget cl-ironclad cl-babel \
 && rm -rf /var/lib/apt/lists/*

# Copy only the files the CL build needs — not clojure/ or test/.
# This way, editing Clojure source does not invalidate this expensive layer.
COPY src/        /opt/src/pgloader/src/
COPY conf/       /opt/src/pgloader/conf/
COPY Makefile    /opt/src/pgloader/Makefile
COPY pgloader.asd /opt/src/pgloader/pgloader.asd

ARG DYNSIZE=16384

RUN mkdir -p /opt/src/pgloader/build/bin \
 && cd /opt/src/pgloader \
 && make DYNSIZE=$DYNSIZE clones save

# ── Stage 2: test-runner image (JRE + psql + make + pgloader v3) ──────────────
FROM clojure:temurin-21-tools-deps-bookworm

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      postgresql-client make libsybdb5 unzip \
 && rm -rf /var/lib/apt/lists/*

# Include the CL pgloader binary so update-expected targets can run v3 to
# generate the "golden" expected/ files that v4 tests compare against.
COPY --from=pgloader-v3-builder /opt/src/pgloader/build/bin/pgloader \
     /usr/local/bin/pgloader
COPY --from=pgloader-v3-builder /opt/src/pgloader/conf/freetds.conf \
     /etc/freetds/freetds.conf

# ── ~/.my.cnf for issue #767 test ────────────────────────────────────────────
# Copied from the repo as a plain file; installed as a dotfile at the right
# location.  chmod 600 is required — MySQL refuses world-readable option files.
COPY clojure/tests/mysql/my-cnf/my.cnf /root/.my.cnf
RUN chmod 600 /root/.my.cnf
