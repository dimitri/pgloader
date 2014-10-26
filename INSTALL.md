# Installing pgloader

pgloader version 3.x is written in Common Lisp.

## The lisp parts

The steps depend on the OS you are currently using.

### debian

If you're using debian, it's quite simple actually, see the file
`bootstrap-debian.sh` within the main pgloader distribution to get yourself
started.

You will note in particular:

    sudo apt-get install -y sbcl                                  \
                            git curl patch unzip                  \
                            devscripts pandoc                     \
                            libsqlite3-dev

We need a recent enough [SBCL](http://sbcl.org/) version and that means
backporting the one found in `sid` rather than using the very old one found
in current *stable* debian release. See `bootstrap-debian.sh` for details
about how to backport a recent enough SBCL here (1.1.14 or newer).

### Mac OS X

We suppose you already have `git` and `make` available, if that's not the
case now is the time to install those tools. The SQLite lib that comes in
MacOSX is fine, no need for extra software here.

You will need to install either SBCL or CCL separately, and when using
[brew](http://brew.sh/) it's as simple as:

    brew install sbcl
    brew install clozure-cl

### Compiling SBCL by yourself

If you ended up building SBCL yourself or you just want to do that, you can
download the source from http://www.sbcl.org/ .

You will need to build SBCL with the following command and options:

    sh make.sh --with-sb-core-compression --with-sb-thread
    
NOTE: You could also remove the --compress-core option.


## Building pgloader

Now that the dependences are installed, just type make.

    make

If using Mac OS X, and depending on how you did install `SBCL` and which
version you have (the brew default did change recently), you might need to
ask the Makefile to refrain from trying to compress your binary image:

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
