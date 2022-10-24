Summary:            extract, transform and load data into PostgreSQL
Name:               pgloader
Version:            3.6.9
Release:            22%{?dist}
License:            The PostgreSQL Licence
Group:              System Environment/Base
URL:                https://github.com/dimitri/pgloader
Source0:            %{url}/archive/v%{version}.tar.gz

BuildRequires: sbcl
BuildRequires: freetds-devel
BuildRequires: openssl-devel
BuildRequires: sqlite-devel
BuildRequires: zlib-devel
Requires: freetds
Requires: openssl-devel
Requires: sbcl
Requires: zlib
Requires: sqlite

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
%setup -q -n %{name}-%{version}

%build
%define debug_package %{nil}
make pgloader

%install
install -m 755 -d %{buildroot}/%{_bindir}
cp build/bin/pgloader %{buildroot}/%{_bindir}/pgloader
mkdir -p $RPM_BUILD_ROOT/etc/prelink.conf.d
echo '-b /usr/bin/pgloader' > $RPM_BUILD_ROOT/etc/prelink.conf.d/%{name}.conf

%files
%doc README.md
%{_bindir}/*
/etc/prelink.conf.d/%{name}.conf

%changelog
* Sun Mar 22 2020 Michał "phoe" Herda <phoe@disroot.org> - 3.6.2
- Release 3.6.2

* Tue Sep 24 2019 Phil Ingram <pingram.au@gmail.com> - 3.6.1
- Release 3.6.1
- Use Requires and BuildRequires
- Variablise Source0
- Fix Files

* Thu Jan 22 2015 Dimitri Fontaine <dimitri@2ndQuadrant.fr> - 3.2.1.preview-22
- Release 3.2.1.preview

* Thu Jan 15 2015 Dimitri Fontaine <dimitri@2ndQuadrant.fr> - 3.2.0-22
- Release 3.2.0

* Wed Nov 5 2014 Dimitri Fontaine <dimitri@2ndQuadrant.fr> - 3.1.1-22
- Release 3.1.1

* Wed Sep 10 2014 Dimitri Fontaine <dimitri@2ndQuadrant.fr> - 3.1.0-22
- Release 3.1.0

* Tue Apr 29 2014 Dimitri Fontaine <dimitri@2ndQuadrant.fr> 3.0.99
- Assorted fixes, release candidate 9
* Mon Dec 23 2013 Dimitri Fontaine <dimitri@2ndQuadrant.fr> 3.0.98
- Assorted fixes, release candidate 8
* Sun Dec 15 2013 Dimitri Fontaine <dimitri@2ndQuadrant.fr> 3.0.97
- Assorted fixes, release candidate 7
* Tue Dec 10 2013 Dimitri Fontaine <dimitri@2ndQuadrant.fr> 3.0.96
- Package as an RPM

%global __os_install_post %{nil}
