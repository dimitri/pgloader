#!/usr/bin/env bash

sudo yum -y install yum-utils rpmdevtools @"Development Tools" \
                        sqlite-devel zlib-devel

# Enable epel for sbcl
sudo yum -y install epel-release
sudo yum -y install sbcl 

# Missing dependency
sudo yum install freetds freetds-devel -y
sudo ln -s /usr/lib64/libsybdb.so.5 /usr/lib64/libsybdb.so

# prepare the rpmbuild setup
rpmdev-setuptree
