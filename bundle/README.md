# pgloader source bundle

In order to ease building pgloader for non-lisp users, the *bundle*
distribution is a tarball containing pgloader and its build dependencies.
See the the following documentation for more details:

  <https://www.quicklisp.org/beta/bundles.html>

The *bundle* comes with a specific `Makefile` so that building it is as
simple as the following (which includes testing the resulting binary):

    make
    LANG=en_US.UTF-8 make test

The compilation might takes a while, it's because SBCL is trying hard to
generate run-time binary code that is fast and efficient. Yes you need to be
in a unicide environment to run the test suite, so that it matches with the
encoding of the test *.load files.

You can then package or use the pgloader binary:

    ./bin/pgloader --version
    ./bin/pgloader --help

Note that the SQLite test files are not included in the bundle, for weithing
too much here.
