FROM debian:stretch
MAINTAINER Dimitri Fontaine <dim@tapoueh.org>

RUN apt-get update                                   && \
    apt-get install -y --no-install-recommends          \
                    wget curl make git bzip2 time       \
                    ca-certificates                     \
                    libzip-dev libssl1.1 openssl        \
                    patch unzip libsqlite3-dev gawk     \
                    freetds-dev sbcl                 && \
    rm -rf /var/lib/apt/lists/*

ADD ./ /opt/src/pgloader
WORKDIR /opt/src/pgloader

# build/ is in the .dockerignore file, but we actually need it now
RUN mkdir -p build/bin
RUN make

RUN cp /opt/src/pgloader/build/bin/pgloader /usr/local/bin
