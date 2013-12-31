#!/usr/bin/env bash

sudo yum -y install yum-utils @development-tools sbcl sqlite-devel

# SBCL 1.1.14
# http://www.mikeivanov.com/post/66510551125/installing-sbcl-1-1-on-rhel-centos-systems
sudo yum -y groupinstall "Development Tools"
wget http://dl.fedoraproject.org/pub/epel/6/x86_64/epel-release-6-8.noarch.rpm
sudo rpm -Uvh epel-release-6*.rpm 
sudo yum install -y sbcl.x86_64

wget http://downloads.sourceforge.net/project/sbcl/sbcl/1.1.14/sbcl-1.1.14-source.tar.bz2
tar xfj sbcl-1.1.14-source.tar.bz2
cd sbcl-1.1.14
./make.sh --with-sb-thread --with-sb-core-compression > /dev/null 2>&1
sudo sh install.sh
cd

# remove the old version that we used to compile the newer one.
sudo yum remove -y sbcl

# pgloader
#make -C /vagrant rpm
