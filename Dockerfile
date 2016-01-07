FROM debian:jessie
MAINTAINER Dimitri Fontaine <dim@tapoueh.org>

RUN apt-get update
RUN apt-get install -y wget curl make git bzip2 time libzip-dev libssl1.0.0 openssl
RUN apt-get install -y patch unzip libsqlite3-dev gawk freetds-dev sbcl

ADD ./ /opt/src/pgloader
WORKDIR /opt/src/pgloader

# build/ is in the .dockerignore file, but we actually need it now
RUN mkdir -p build/bin
RUN make

RUN cp /opt/src/pgloader/build/bin/pgloader /usr/local/bin
