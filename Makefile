DOCS = pgloader.1.sgml
GARBAGE = manpage.links manpage.refs

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

man: $(DOCS)
	docbook2man $(DOCS) 2>/dev/null
	-rm -f $(GARBAGE)
