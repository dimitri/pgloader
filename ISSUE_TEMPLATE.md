Thanks for contributing to [pgloader](https://pgloader.io) by reporting an
issue! Reporting an issue is the only way we can solve problems, fix bugs,
and improve both the software and its user experience in general.

The best bug reports follow those 3 simple steps:

  1. show what you did,
  2. show the result you got,
  3. explain how the result is not what you expected.

In the case of pgloader, here's the information I will need to read in your
bug report. Having all of this is a big help, and often means the bug you
reported can be fixed very efficiently as soon as I get to it.

Please provide the following information:

  - [ ] pgloader --version
  
  - [ ] did you test a fresh compile from the source tree?
    
    Compiling pgloader from sources is documented in the
    [README](https://github.com/dimitri/pgloader#build-from-sources), it's
    easy to do, and if patches are to be made to fix your bug, you're going
    to have to build from sources to get the fix anyway…
    
  - [ ] did you search for other similar issues?
    
  - [ ] how can I reproduce the bug?
  
    Have a look at the
    [test](https://github.com/dimitri/pgloader/tree/master/test) files in
    the pgloader repository, such as the
    [csv-error.load](https://github.com/dimitri/pgloader/blob/master/test/csv-error.load)
    file for example.
    
    You will notice it's using the FROM INLINE clause. The file contains the
    sample data set we need to reproduce your bug! Also, notice the BEFORE
    LOAD DO clause that begins with a DROP TABLE IF EXISTS statement and
    continues with a CREATE TABLE command. This file is self-contained. All
    we need to reproduce the behavior is a single pgloader command file!
    
    If you're loading from a database, consider attaching a database dump to
    your issue. For MySQL, use `mysqldump`. For SQLite, just send over your
    source file, that's easy. Maybe be the one with your production data, of
    course, the one with just the sample of data that allows me to reproduce
    your bug.
    
    When using a proprietary database system as a source, consider creating
    a sample database on some Cloud service or somewhere you can then give
    me access to, and see my email address on my GitHub profile to send me
    the credentials. Still open a public issue for tracking and as
    documentation for other users.
