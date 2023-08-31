FROM debian:stable-slim as builder

  RUN apt-get update && apt-get install -y wget
  RUN wget http://archive.ubuntu.com/ubuntu/pool/main/o/openssl/libssl1.1_1.1.1-1ubuntu2.1~18.04.23_amd64.deb
  RUN dpkg -i libssl1.1_1.1.1-1ubuntu2.1~18.04.23_amd64.deb

  RUN apt-get update \
      && apt-get install -y --no-install-recommends \
        bzip2 \
        ca-certificates \
        curl \
        freetds-dev \
        gawk \
        git \
        libsqlite3-dev \
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

FROM debian:stable-slim

  RUN apt-get update \
      && apt-get install -y --no-install-recommends \
        curl \
        freetds-dev \
        gawk \
        libsqlite3-dev \
        libzip-dev \
        make \
        sbcl \
        unzip \
      && rm -rf /var/lib/apt/lists/*

  COPY --from=builder /opt/src/pgloader/build/bin/pgloader /usr/local/bin

  ADD conf/freetds.conf /etc/freetds/freetds.conf

  LABEL maintainer="Dimitri Fontaine <dim@tapoueh.org>"
