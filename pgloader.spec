Summary:            extract, transform and load data into PostgreSQL
Name:               pgloader
Version:            3.0.96
Release:            21%{?dist}
License:            The PostgreSQL Licence
Group:              System Environment/Base
Source:             %{name}-%{version}.tar.gz
URL:                https://github.com/dimitri/pgloader

%description
pgloader imports data from different kind of sources and COPY it into
PostgreSQL.

The command language is described in the manual page and allows to describe
where to find the data source, its format, and to describe data processing
and transformation.
 
Supported source formats include CSV, fixed width flat files, dBase3 files
(DBF), and SQLite and MySQL databases. In most of those formats, pgloader is
able to auto-discover the schema and create the tables and the indexes in
PostgreSQL. In the MySQL case it's possible to edit CASTing rules from the
pgloader command directly.

%prep
%setup -q -n %{name}

%build
%define debug_package %{nil}
make pgloader

%install
install -m 755 -d %{buildroot}/%{_bindir}
cp build/pgloader.exe %{buildroot}/%{_bindir}/pgloader

%files
%doc README.md pgloader.1.md
%{_bindir}/*

%changelog
* Tue Dec 31 2013 Dimitri Fontaine <dimitri@2ndQuadrant.fr> 3.0.96
- Package as an RPM

%global __os_install_post %{nil}
