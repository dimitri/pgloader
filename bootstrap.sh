#!/usr/bin/env bash

echo "deb http://apt.postgresql.org/pub/repos/apt/ wheezy-pgdg main" > /etc/apt/sources.list.d/pgdg.list

wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add -

apt-get update
apt-get install -y postgresql-9.3 postgresql-contrib-9.3
apt-get install -y sbcl libmysqlclient-dev git patch unzip

HBA=/etc/postgresql/9.3/main/pg_hba.conf
echo "local all all trust"              | sudo tee $HBA
echo "host  all all 127.0.0.1/32 trust" | sudo tee -a $HBA

sudo pg_ctlcluster 9.3 main reload
createuser -U postgres -SdR `whoami`
createdb pgloader

wget --quiet http://beta.quicklisp.org/quicklisp.lisp
sbcl --load quicklisp.lisp <<EOF
(quicklisp-quickstart:install)
(ql:add-to-init-file)

(ql:quickload "buildapp")
(buildapp:build-buildapp "/home/vagrant/buildapp")
(quit)
EOF

cd ~/quicklisp/local-projects/
git clone -b empty-strings-and-nil https://github.com/dimitri/cl-csv.git
git clone https://github.com/marijnh/Postmodern.git

cd ~/quicklisp/local-projects/Postmodern/
patch -p1 < /vagrant/patches/postmodern-send-copy-done.patch

cd ~

sbcl --load quicklisp/setup.lisp <<EOF
(pushnew #p"/vagrant/" asdf:*central-registry*)
(pushnew #p"/vagrant/lib/db3/" asdf:*central-registry*)
(pushnew #p"/vagrant/lib/abnf/" asdf:*central-registry*)
(ql:quickload "pgloader")
(quit)
EOF

REGISTRY=~/.config/common-lisp/source-registry.conf.d
mkdir -p $REGISTRY
echo "(:tree \"/vagrant/\")" > $REGISTRY/projects.conf

# echo "BUILDING PGLOADER SELF-CONTAINED BINARY"

# /home/vagrant/buildapp --logfile /tmp/build.log        \
#                        --asdf-tree ~/quicklisp/dists   \
#                        --asdf-tree /vagrant            \
#                        --load-system pgloader          \
#                        --entry pgloader:main           \
#                        --dynamic-space-size 4096       \
#                        --output /home/vagrant/pgloader.exe

echo "TESTING"

/vagrant/pgloader.lisp --help

for test in /vagrant/test/*.load
do
    echo "# TEST: $test"
    echo
    /vagrant/pgloader.lisp $test
    echo
done
