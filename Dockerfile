FROM debian:jessie
MAINTAINER Dimitri Fontaine <dim@tapoueh.org>

RUN apt-get update
RUN apt-get install -y wget curl make git bzip2 time libzip-dev libssl1.0.0 openssl
RUN apt-get install -y patch unzip libsqlite3-dev gawk freetds-dev sbcl

ADD ./ /opt/src/pgloader

# we have build in the .dockerignore file, but we actually need it now
RUN mkdir -p /opt/src/pgloader/build/bin
RUN make PWD=/opt/src/pgloader -C /opt/src/pgloader

RUN cp /opt/src/pgloader/build/bin/pgloader /usr/local/bin
