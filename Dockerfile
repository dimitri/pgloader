FROM debian:bookworm-slim AS builder

  RUN apt-get update \
      && apt-get install -y --no-install-recommends \
        bzip2 \
        ca-certificates \
        curl \
        freetds-dev \
        gawk \
        git \
        libsqlite3-dev \
        libssl3 \
        libzip-dev \
        make \
        openssl \
        patch \
        sbcl \
        time \
        unzip \
        wget \
        cl-ironclad \
        cl-babel \
      && rm -rf /var/lib/apt/lists/*

  COPY ./ /opt/src/pgloader

ARG DYNSIZE=16384

  RUN mkdir -p /opt/src/pgloader/build/bin \
      && cd /opt/src/pgloader \
      && make DYNSIZE=$DYNSIZE clones save

FROM debian:bookworm-slim

  RUN apt-get update \
      && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        freetds-dev \
        gawk \
        libsqlite3-dev \
        libzip-dev \
        make \
        sbcl \
        unzip \
      && update-ca-certificates \
      && rm -rf /var/lib/apt/lists/*

  COPY --from=builder /opt/src/pgloader/build/bin/pgloader /usr/local/bin

  ADD conf/freetds.conf /etc/freetds/freetds.conf

  LABEL maintainer="Dimitri Fontaine <dim@tapoueh.org>"
