Installing pgloader
===================

Several distributions are available for pgcopydb.

debian packages
---------------

You can install pgloader directly from `apt.postgresql.org`__ and from
official debian repositories, see `packages.debian.org/pgloader`__.

::
   
    $ apt-get install pgloader

__ https://wiki.postgresql.org/wiki/Apt
__ https://packages.debian.org/search?keywords=pgloader

RPM packages
------------

The Postgres community repository for RPM packages is `yum.postgresql.org`__
and does include binary packages for pgloader.

__ https://yum.postgresql.org

Docker Images
-------------

Docker images are maintained for each tagged release at dockerhub, and also
built from the CI/CD integration on GitHub at each commit to the `main`
branch.

The DockerHub `dimitri/pgloader`__ repository is where the tagged releases
are made available. The image uses the Postgres version currently in debian
stable.

__ https://hub.docker.com/r/dimitri/pgloader

To use the ``dimitri/pgloader`` docker image::

  $ docker run --rm -it dimitri/pgloader:latest pgloader --version

Or you can use the CI/CD integration that publishes packages from the main
branch to the GitHub docker repository::

  $ docker pull ghcr.io/dimitri/pgloader:latest
  $ docker run --rm -it ghcr.io/dimitri/pgloader:latest pgloader --version
  $ docker run --rm -it ghcr.io/dimitri/pgloader:latest pgloader --help
    
Build from sources
------------------

pgloader is a Common Lisp program, tested using the `SBCL`__ (>= 1.2.5) and
`Clozure CL`__ implementations and with `Quicklisp`__ to fetch build
dependencies.

__ http://sbcl.org/
__ http://ccl.clozure.com/
__ http://www.quicklisp.org/beta/

When building from sources, you should always build from the current git
HEAD as it's basically the only source that is managed in a way to ensure it
builds aginst current set of dependencies versions.

The build system for pgloader uses a Makefile and the Quicklisp Common Lisp
packages distribution system.

The modern build system for pgloader is entirely written in Common Lisp,
where the historical name for our operation is `save-lisp-and-die` and can
be used that way:

::

   $ make save

The legacy build system also uses Buildapp and can be used that way:

::

   $ make pgloader

Building from sources on debian
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Install the build dependencies first, then use the Makefile::

    $ apt-get install sbcl unzip libsqlite3-dev make curl gawk freetds-dev libzip-dev
    $ cd /path/to/pgloader

    $ make save
    $ ./build/bin/pgloader --help

Building from sources on RedHat/CentOS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To build and install pgloader the Steel Bank Common Lisp package (sbcl) from
EPEL, and the freetds packages are required.

It is recommended to build the RPM yourself, see below, to ensure that all
installed files are properly tracked and that you can safely update to newer
versions of pgloader as they're released.

To do an adhoc build and install run ``boostrap-centos.sh`` for CentOS 6 or
``bootstrap-centos7.sh`` for CentOS 7 to install the required dependencies.

Building a pgloader RPM from sources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The spec file in the root of the pgloader repository can be used to build your
own RPM. For production deployments it is recommended that you build this RPM on
a dedicated build box and then copy the RPM to your production environment for
use; it is considered bad practice to have compilers and build tools present in
production environments.

1. Install the [EPEL repo](https://fedoraproject.org/wiki/EPEL#Quickstart).

2. Install rpmbuild dependencies::

        sudo yum -y install yum-utils rpmdevtools @"Development Tools"

3. Install pgloader build dependencies::

        sudo yum-builddep pgloader.spec

4. Download pgloader source::

        spectool -g -R pgloader.spec

5. Build the source and binary RPMs (see `rpmbuild --help` for other build
   options)::

        rpmbuild -ba pgloader.spec

Building from sources on macOS
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

We suppose you already have ``git`` and ``make`` available, if that's not
the case now is the time to install those tools. The SQLite lib that comes
in MacOSX is fine, no need for extra software here.

You will need to install either SBCL or CCL separately, and when using
[brew](http://brew.sh/) it's as simple as:

::
   
   $ brew install sbcl
   $ brew install clozure-cl

NOTE: Make sure you installed the universal binaries of Freetds, so that
they can be loaded correctly.

::
   
   $ brew install freetds --universal --build-from-source

Then use the normal build system for pgloader:

::

   $ make save
   $ ./build/bin/pgloader --version

Building from sources on Windows
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Building pgloader on Windows is supported (in theory), thanks to Common Lisp
implementations being available on that platform, and to the Common Lisp
Standard for making it easy to write actually portable code.

It is recommended to have a look at the `issues labelled with Windows
support`__ if you run into trouble when building pgloader, because the
development team is lacking windows user and in practice we can't maintain
the support for that Operating System:

__ https://github.com/dimitri/pgloader/issues?utf8=✓&q=label%3A%22Windows%20support%22%20>

If you need ``pgloader.exe`` on windows please condider contributing fixes
for that environment and maybe longer term support then. Specifically, a CI
integration with a windows build host would allow ensuring that we continue
to support that target.

Building Docker image from sources
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can build a Docker image from source using SBCL by default::

  $ docker build .

Or Clozure CL (CCL)::

  $ docker build -f Dockerfile.ccl .

More options when building from source
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The ``Makefile`` target ``save`` knows how to produce a Self Contained
Binary file for pgloader, found at ``./build/bin/pgloader``::

    $ make save

By default, the ``Makefile`` uses `SBCL`__ to compile your binary image,
though it's possible to build using `Clozure-CL`__.

__ http://sbcl.org/
__ http://ccl.clozure.com/

::
   
   $ make CL=ccl64 save

It is possible to to tweak the default amount of memory that the pgloader
image will allow itself using when running through your data (don't ask for
more than your current RAM tho). At the moment only the legacy build system
includes support for this custom build::

    $ make DYNSIZE=8192 pgloader

The ``make pgloader`` command when successful outputs a
`./build/bin/pgloader` file for you to use.

