#!/usr/bin/env bash

if [ ! -f /etc/apt/sources.list.old ]
then
    sudo mv /etc/apt/sources.list /etc/apt/sources.list.old
    sudo cp /vagrant/conf/sources.list /etc/apt/sources.list
fi

sudo apt-get update
sudo apt-get dist-upgrade -y

cat /vagrant/conf/bashrc.sh >> ~/.bashrc

# PostgreSQL
sidsrc=/etc/apt/sources.list.d/sid-src.list
echo "deb-src http://ftp.fr.debian.org/debian/ sid main" | sudo tee $sidsrc

pgdg=/etc/apt/sources.list.d/pgdg.list
pgdgkey=https://www.postgresql.org/media/keys/ACCC4CF8.asc
echo "deb http://apt.postgresql.org/pub/repos/apt/ wheezy-pgdg main" | sudo tee $pgdg

wget --quiet -O - ${pgdgkey} | sudo apt-key add -

# MariaDB
sudo apt-get install -y python-software-properties
sudo apt-key adv --recv-keys --keyserver keyserver.ubuntu.com 0xcbcb082a1bb943db
sudo add-apt-repository 'deb http://mirrors.linsrv.net/mariadb/repo/10.0/debian wheezy main'

sudo apt-get update
sudo apt-get install -y postgresql-15                         \
                        postgresql-15-ip4r                    \
                        sbcl                                  \
                        git patch unzip                       \
                        devscripts pandoc                     \
                        freetds-dev libsqlite3-dev            \
                        gnupg gnupg-agent

sudo DEBIAN_FRONTEND=noninteractive \
     apt-get install -y --allow-unauthenticated mariadb-server

# SBCL
#
# we used to need to backport SBCL, it's only the case now in wheezy, all
# the later distributions are uptodate enough for our needs here.
sudo apt-get -y install sbcl

HBA=/etc/postgresql/9.3/main/pg_hba.conf
echo "local all all trust"              | sudo tee $HBA
echo "host  all all 127.0.0.1/32 trust" | sudo tee -a $HBA

sudo pg_ctlcluster 9.3 main reload
createuser -U postgres -SdR `whoami`

make -C /vagrant pgloader
make -C /vagrant test
