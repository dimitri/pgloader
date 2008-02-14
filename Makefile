# $Id: Makefile,v 1.12 2008-02-14 23:09:06 dim Exp $
#
# Makefile for debian packaging purpose, make install not intended to work.

DOCS    = pgloader.1.txt
TODO    = TODO.txt
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
EXPORT = $(DEBDIR)/export/pgloader-$(SHORTVER)
ORIG   = $(DEBDIR)/export/pgloader_$(VERSION).orig.tar.gz
ARCHIVE= $(DEBDIR)/export/pgloader-$(SHORTVER).tar.gz

install:
	install -m 755 $(pgloader) $(DESTDIR)/usr/bin/pgloader
	install -m 755 -d $(libdir)/pgloader
	install -m 755 -d $(libdir)/reformat

	cp -a $(libs) $(libdir)/pgloader
	cp -a $(refm) $(libdir)/reformat
	cp -a $(examples) $(exdir)

html: $(DOCS)
	asciidoc -a toc $<

todo: $(TODO)
	asciidoc -a toc $<

site: html
	scp ${DOCS:.txt=.html} cvs.pgfoundry.org:htdocs

pgloader.1.xml: $(DOCS)
	asciidoc -d manpage -b docbook $<

man: ${DOCS:.txt=.xml}
	xmlto man $<

deb:
	# working copy from where to make the .orig archive
	rm -rf $(DEBDIR)	
	mkdir -p $(DEBDIR)/pgloader-$(SHORTVER)
	mkdir -p $(EXPORT)
	cp -a . $(EXPORT)
	for n in ".#*" "*~" "*.pyc" "build-stamp" "configure-stamp"; do \
	  find $(EXPORT) -name "$$n" -print0|xargs -0 echo rm -f; \
	  find $(EXPORT) -name "$$n" -print0|xargs -0 rm -f; \
	done
	find $(EXPORT) -type d -name CVS -print0|xargs -0 rm -rf

	# prepare the .orig without the debian/ packaging stuff
	cp -a $(EXPORT) $(DEBDIR)
	rm -rf $(DEBDIR)/pgloader-$(SHORTVER)/debian
	(cd $(DEBDIR) && tar czf $(ORIG) pgloader-$(SHORTVER))

	# have a copy of the $ORIG file named $ARCHIVE for non-debian packagers
	cp $(ORIG) $(ARCHIVE)

	# build the debian package and copy them to ..
	(cd $(EXPORT) && debuild)
	cp -a $(DEBDIR)/export/pgloader[_-]$(VERSION)* ..
	cp -a $(ARCHIVE) ..
