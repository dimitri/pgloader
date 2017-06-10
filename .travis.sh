#!/bin/bash

set -eu

pgdg_repository() {
	local sourcelist='sources.list.d/pgdg.list'

	sudo apt-key adv --keyserver 'hkp://ha.pool.sks-keyservers.net' --recv-keys 'ACCC4CF8'
	echo deb http://apt.postgresql.org/pub/repos/apt/ trusty-pgdg main | sudo tee "/etc/apt/$sourcelist"
	sudo apt-get -o Dir::Etc::sourcelist="$sourcelist" -o Dir::Etc::sourceparts='-' -o APT::Get::List-Cleanup='0' update
}

postgresql_configure() {
	sudo tee /etc/postgresql/9.1/main/pg_hba.conf > /dev/null <<-config
		local  all  all                trust
		host   all  all  127.0.0.1/32  trust
	config

	sudo service postgresql restart
}

postgresql_uninstall() {
	sudo service postgresql stop
	xargs sudo apt-get -y --purge remove <<-packages
		libpq-dev
		libpq5
		postgresql
		postgresql-client-common
		postgresql-common
	packages
}

remote_file() {
	local target="$1" origin="$2" sum="$3"
	local check="shasum --algorithm $(( 4 * ${#sum} )) --check"
	local filesum="$sum  $target"

	curl --location --output "$target" "$origin" && $check <<< "$filesum"
}

sbcl_install() {
	sbcl_checksum='eb44d9efb4389f71c05af0327bab7cd18f8bb221fb13a6e458477a9194853958'
	sbcl_version='1.3.18'

	remote_file "/tmp/sbcl-${sbcl_version}.tgz" "http://prdownloads.sourceforge.net/sbcl/sbcl-${sbcl_version}-x86-64-linux-binary.tar.bz2" "$sbcl_checksum"
	tar --file  "/tmp/sbcl-${sbcl_version}.tgz" --extract --directory '/tmp'
	( cd "/tmp/sbcl-${sbcl_version}-x86-64-linux" && sudo ./install.sh )
}

$1
