#!/usr/bin/env bash

pgdg=/etc/apt/sources.list.d/pgdg.list
pgdgkey=https://www.postgresql.org/media/keys/ACCC4CF8.asc
echo "deb http://apt.postgresql.org/pub/repos/apt/ wheezy-pgdg main" | sudo tee $pgdg

wget --quiet -O - ${pgdgkey} | sudo apt-key add -

sudo apt-get update
sudo apt-get install -y postgresql-9.3 postgresql-contrib-9.3 \
                        postgresql-9.3-ip4r                   \
                        sbcl                                  \
                        git patch unzip                       \
                        libmysqlclient-dev libsqlite3-dev

HBA=/etc/postgresql/9.3/main/pg_hba.conf
echo "local all all trust"              | sudo tee $HBA
echo "host  all all 127.0.0.1/32 trust" | sudo tee -a $HBA

sudo pg_ctlcluster 9.3 main reload
createuser -U postgres -SdR `whoami`

make -C /vagrant pgloader
make -C /vagrant test
