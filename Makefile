# $Id: Makefile,v 1.19 2008-06-03 12:58:14 dim Exp $
#
# Makefile for debian packaging purpose, make install not intended to work.

DOCS    = pgloader.1.txt
TODO    = TODO.txt
BUGS    = BUGS.txt
CVSROOT = $(shell cat CVS/Root)
VERSION = $(shell ./pgloader.py --version |cut -d' ' -f3)
SHORTVER= $(shell ./pgloader.py --version |cut -d' ' -f3 |cut -d '~' -f1)

# debian setting
DESTDIR =

libdir   = $(DESTDIR)/usr/share/python-support/pgloader
exdir    = $(DESTDIR)/usr/share/doc/pgloader

pgloader = pgloader.py
examples = examples
libs = $(wildcard pgloader/*.py)
refm = $(wildcard reformat/*.py)

DEBDIR = /tmp/pgloader
EXPORT = $(DEBDIR)/export/pgloader-$(VERSION)
ORIG   = $(DEBDIR)/export/pgloader_$(VERSION).orig.tar.gz
ARCHIVE= $(DEBDIR)/export/pgloader-$(VERSION).tar.gz

install:
	install -m 755 $(pgloader) $(DESTDIR)/usr/bin/pgloader
	install -m 755 -d $(libdir)/pgloader
	install -m 755 -d $(libdir)/reformat

	cp -a $(libs) $(libdir)/pgloader
	cp -a $(refm) $(libdir)/reformat
	cp -a $(examples) $(exdir)
	cp -a $(TODO) $(BUGS) $(DESTDIR)/usr/share/doc/pgloader

html: $(DOCS)
	asciidoc -a toc $<

todo: $(TODO)
	asciidoc -a toc $<

bugs: $(BUGS)
	asciidoc -a toc $<

site: html
	scp ${DOCS:.txt=.html} cvs.pgfoundry.org:htdocs

pgloader.1.xml: $(DOCS)
	asciidoc -d manpage -b docbook $<

man: ${DOCS:.txt=.xml}
	xmlto man $<

doc: man html todo bugs

clean:
	rm -f *.xml *.html *.1 *~

deb:
	# working copy from where to make the .orig archive
	rm -rf $(DEBDIR)	
	mkdir -p $(DEBDIR)/pgloader-$(VERSION)
	mkdir -p $(EXPORT)
	rsync -Ca . $(EXPORT)

	# get rid of temp and build files
	for n in ".#*" "*~" "*.pyc" "build-stamp" "configure-stamp" "parallel.o*"; do \
	  find $(EXPORT) -name "$$n" -print0|xargs -0 echo rm -f; \
	  find $(EXPORT) -name "$$n" -print0|xargs -0 rm -f; \
	done

	# prepare the .orig without the debian/ packaging stuff
	cp -a $(EXPORT) $(DEBDIR)
	rm -rf $(DEBDIR)/pgloader-$(VERSION)/debian
	(cd $(DEBDIR) && tar czf $(ORIG) pgloader-$(VERSION))

	# have a copy of the $ORIG file named $ARCHIVE for non-debian packagers
	cp $(ORIG) $(ARCHIVE)

	# build the debian package and copy them to ..
	(cd $(EXPORT) && debuild)
	cp -a $(DEBDIR)/export/pgloader[_-]$(VERSION)* ..
	cp -a $(ARCHIVE) ..
