DOCS = pgloader.1.txt

# debian setting
DESTDIR =

libdir   = $(DESTDIR)/usr/share/pgloader
exdir    = $(DESTDIR)/usr/share/doc/pgloader

pgloader = pgloader.py
examples = examples
libs = $(wildcard pgloader/*.py)
refm = $(wildcard reformat/*.py)

install:
	install -m 755 $(pgloader) $(libdir)
	install -m 755 -d $(libdir)/pgloader
	install -m 755 -d $(libdir)/reformat

	cp -a $(libs) $(libdir)/pgloader
	cp -a $(refm) $(libdir)/reformat
	cp -a $(examples) $(exdir)

html: $(DOCS)
	asciidoc -a toc $<

pgloader.1.xml: $(DOCS)
	asciidoc -d manpage -b docbook $<

man: ${DOCS:.txt=.xml}
	xmlto man $<
