DOCS = pgloader.1.txt

# debian setting
DESTDIR =

libdir   = $(DESTDIR)/usr/share/pgloader
exdir    = $(DESTDIR)/usr/share/doc/pgloader

pgloader = pgloader.py
examples = examples
libs = $(wildcard pgloader/*.py)

install:
	install -m 755 $(pgloader) $(libdir)
	install -m 755 -d $(libdir)/pgloader

	cp -a $(libs) $(libdir)/pgloader
	cp -a $(examples) $(exdir)

html: $(DOCS)
	asciidoc -a toc $<

pgloader.1.xml: $(DOCS)
	asciidoc -d manpage -b docbook $<

man: pgloader.1.xml
	xmlto man $<
