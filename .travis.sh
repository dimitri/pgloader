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

$1
