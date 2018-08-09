FROM debian:stretch

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
      bzip2 \
      ca-certificates \
      curl \
      freetds-dev \
      gawk \
      git \
      libsqlite3-dev \
      libssl1.1 \
      libzip-dev \
      make \
      openssl \
      patch \
      sbcl \
      time \
      unzip \
      wget \
    && rm -rf /var/lib/apt/lists/*

COPY ./ /opt/src/pgloader

RUN mkdir -p /opt/src/pgloader/build/bin \
    && cd /opt/src/pgloader \
    && make \
    && mv /opt/src/pgloader/build/bin/pgloader /usr/local/bin

LABEL maintainer="Dimitri Fontaine <dim@tapoueh.org>"