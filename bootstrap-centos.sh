#!/usr/bin/env bash

sudo yum -y install yum-utils rpmdevtools @"Development Tools" \
                    sqlite-devel zlib-devel

# SBCL 1.3, we'll overwrite the repo version of sbcl with a more recent one
sudo yum -y install epel-release
sudo yum install -y sbcl.x86_64 --enablerepo=epel

wget http://downloads.sourceforge.net/project/sbcl/sbcl/1.3.6/sbcl-1.3.6-source.tar.bz2
tar xfj sbcl-1.3.6-source.tar.bz2
cd sbcl-1.3.6
./make.sh --with-sb-thread --with-sb-core-compression --prefix=/usr > /dev/null 2>&1
sudo sh install.sh
cd

# Missing dependencies
sudo yum -y install freetds-devel

# prepare the rpmbuild setup
rpmdev-setuptree

# pgloader
#make -C /vagrant rpm
