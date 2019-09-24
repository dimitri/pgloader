# Installing pgloader

pgloader version 3.x is written in Common Lisp.

## Dependencies

The steps depend on the OS you are currently using.

### debian

If you're using debian, it's quite simple actually, see the file
`bootstrap-debian.sh` within the main pgloader distribution to get yourself
started.

You will note in particular:

    sudo apt-get install -y sbcl                                  \
                            git curl patch unzip                  \
                            devscripts pandoc                     \
                            libsqlite3-dev                        \
                            freetds-dev

We need a recent enough [SBCL](http://sbcl.org/) version and that means
backporting the one found in `sid` rather than using the very old one found
in current *stable* debian release. See `bootstrap-debian.sh` for details
about how to backport a recent enough SBCL here (1.2.5 or newer).

### Redhat / CentOS

To build and install pgloader the Steel Bank Common Lisp package (sbcl) from EPEL,
and the freetds packages are required.

With RHEL/CentOS 6, if the packaged version of sbcl isn't >=1.3.6, you'll need
to build it from source.

It is recommended to build the RPM yourself, see below, to ensure that all installed
files are properly tracked and that you can safely update to newer versions of
pgloader as they're released.

To do an adhoc build and install run `boostrap-centos.sh` for CentOS 6 or
`bootstrap-centos7.sh` for CentOS 7 to install the required dependencies.
[Build pgloader](INSTALL.md#building-pgloader).

#### rpmbuild

The spec file in the root of the pgloader repository can be used to build your
own RPM. For production deployments it is recommended that you build this RPM on
a dedicated build box and then copy the RPM to your production environment for
use; it is considered bad practice to have compilers and build tools present in
production environments.

1. Install the [EPEL repo](https://fedoraproject.org/wiki/EPEL#Quickstart).

1. Install rpmbuild dependencies:

        sudo yum -y install yum-utils rpmdevtools @"Development Tools"

1. Install pgloader build dependencies:

        sudo yum-builddep pgloader.spec

1. Download pgloader source:

        spectool -g -R pgloader.spec

1. Build the source and binary RPMs (see `rpmbuild --help` for other build options):

        rpmbuild -ba pgloader.spec

### Mac OS X

We suppose you already have `git` and `make` available, if that's not the
case now is the time to install those tools. The SQLite lib that comes in
MacOSX is fine, no need for extra software here.

You will need to install either SBCL or CCL separately, and when using
[brew](http://brew.sh/) it's as simple as:

    brew install sbcl
    brew install clozure-cl

NOTE: Make sure you installed the universal binaries of Freetds, so that
they can be loaded correctly. 
      
    brew install freetds --universal --build-from-source

### Compiling SBCL by yourself

If you ended up building SBCL yourself or you just want to do that, you can
download the source from http://www.sbcl.org/ .

You will need to build SBCL with the following command and options:

    sh make.sh --with-sb-core-compression --with-sb-thread
    
NOTE: You could also remove the --compress-core option.


## Building pgloader

Now that the dependences are installed, just type make.

    make

If your `SBCL` supports core compression, the make process will use it
to generate a smaller binary.  To force disabling core compression, you
may use:

    make COMPRESS_CORE=no

Then you will have a new tool to play with:

    ./build/bin/pgloader --help
    
This command should spit out the *usage* information on which parameters are
accepted in the command line actually.


## Building pgloader with CCL

It's possible to pick [ccl](http://ccl.clozure.com/) rather than SBCL when
compiling pgloader:

    make CL=ccl

## Building pgloader for use in low RAM environments

It's possible to tweak the size of RAM pgloader will use in its binary
image, at compile time. This defaults to 4 GB.

    make DYNSIZE=1024
    
Now the `./build/bin/pgloader` that you get only uses 1GB.

## Building a docker image

A `Dockerfile` is provided, to use it:

    docker build -t pgloader:debian .
    docker run --rm --name pgloader pgloader:debian bash -c "pgloader --version"

The `build` step install build dependencies in a debian jessie container,
then `git clone` and build `pgloader` in `/opt/src/pgloader` and finally
copy the resulting binary image in `/usr/local/bin/pgloader` so that it's
easily available.
