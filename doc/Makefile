# $Id: Makefile,v 1.1 2005-11-21 16:05:50 jpargudo Exp $

rest = $(wildcard *.rest)
html = $(addsuffix .html, $(basename $(rest)))
pdf  = $(addsuffix .pdf, $(basename $(rest)))

pdf: $(pdf) clean

html: $(html)

dist-clean: clean
	@rm -f $(pdf) $(html)

clean:
	@rm -f *.aux *.log *~ *.tex *.out *.toc *.dvi

%.html: %.rest
	rest2html --stylesheet lib/stylesheet.sty \
		      --no-section-numbering \
		      --language=fr \
		      $< > $@

%.pdf: %.dvi
	dvipdf $<

%.dvi: %.tex
	latex $< >> /dev/null
	latex $< >> /dev/null

%.tex: %.rest 
	rest2latex --use-latex-toc \
	           --stylesheet lib/stylesheet.sty \
	           --use-latex-footnotes \
		   --no-section-numbering \
		   --language=fr \
                   --input-encoding=iso-8859-15 \
		   --table-style=booktabs \
                   --output-encoding=iso-8859-15 \
		       $< > $@
help:
	@echo " Programmes nécessaires: docbook, latex, dvipdf, kpdf"

.PHONY: pdf
