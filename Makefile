# pgloader build tool
APP_NAME   = pgloader
VERSION    = 3.1.1

# use either sbcl or ccl
CL	   = sbcl

# default to 4096 MB of RAM size in the image
DYNSIZE    = 4096

LISP_SRC   = $(wildcard src/*lisp) \
             $(wildcard src/pgsql/*lisp) \
             $(wildcard src/sources/*lisp) \
             pgloader.asd

BUILDDIR   = build
LIBS       = $(BUILDDIR)/libs.stamp
QLDIR      = $(BUILDDIR)/quicklisp
MANIFEST   = $(BUILDDIR)/manifest.ql
LATEST     = $(BUILDDIR)/pgloader-latest.tgz
PGLOADER   = $(BUILDDIR)/bin/$(APP_NAME)

BUILDAPP_CCL  = $(BUILDDIR)/bin/buildapp.ccl
BUILDAPP_SBCL = $(BUILDDIR)/bin/buildapp.sbcl

ifeq ($(CL),sbcl)
BUILDAPP   = $(BUILDAPP_SBCL)
CL_OPTS    = --no-sysinit --no-userinit
else
BUILDAPP   = $(BUILDAPP_CCL)
CL_OPTS    = --no-init
endif

COMPRESS_CORE ?= yes

ifeq ($(CL),sbcl)
ifeq ($(COMPRESS_CORE),yes)
COMPRESS_CORE_OPT = --compress-core
else
COMPRESS_CORE_OPT = 
endif
endif

ifeq ($(CL),sbcl)
BUILDAPP_OPTS =          --require sb-posix                      \
                         --require sb-bsd-sockets                \
                         --require sb-rotate-byte
endif

DEBUILD_ROOT = /tmp/pgloader

all: $(PGLOADER)

clean:
	rm -rf $(LIBS) $(QLDIR) $(MANIFEST) $(BUILDAPP) $(PGLOADER)

docs:
	ronn -roff pgloader.1.md

$(QLDIR)/local-projects/cl-ixf:
	git clone https://github.com/dimitri/cl-ixf.git $@

$(QLDIR)/setup.lisp:
	mkdir -p $(BUILDDIR)
	curl -o $(BUILDDIR)/quicklisp.lisp http://beta.quicklisp.org/quicklisp.lisp
	$(CL) $(CL_OPTS) --load $(BUILDDIR)/quicklisp.lisp                         \
             --eval '(quicklisp-quickstart:install :path "$(BUILDDIR)/quicklisp")' \
             --eval '(quit)'

quicklisp: $(QLDIR)/setup.lisp ;

$(LIBS): $(QLDIR)/setup.lisp $(QLDIR)/local-projects/cl-ixf
	$(CL) $(CL_OPTS) --load $(QLDIR)/setup.lisp                 \
             --eval '(push "$(PWD)/" asdf:*central-registry*)'      \
             --eval '(ql:quickload "pgloader")'                     \
             --eval '(quit)'
	touch $@

libs: $(LIBS) ;

$(MANIFEST): $(LIBS)
	$(CL) $(CL_OPTS) --load $(QLDIR)/setup.lisp                \
             --eval '(ql:write-asdf-manifest-file "$(MANIFEST)")'  \
             --eval '(quit)'

manifest: $(MANIFEST) ;

$(BUILDAPP_CCL): $(QLDIR)/setup.lisp
	mkdir -p $(BUILDDIR)/bin
	$(CL) $(CL_OPTS) --load $(QLDIR)/setup.lisp               \
             --eval '(ql:quickload "buildapp")'                   \
             --eval '(buildapp:build-buildapp "$@")'              \
             --eval '(quit)'

$(BUILDAPP_SBCL): $(QLDIR)/setup.lisp
	mkdir -p $(BUILDDIR)/bin
	$(CL) $(CL_OPTS) --load $(QLDIR)/setup.lisp               \
             --eval '(ql:quickload "buildapp")'                   \
             --eval '(buildapp:build-buildapp "$@")'              \
             --eval '(quit)'

buildapp: $(BUILDAPP) ;

$(PGLOADER): $(MANIFEST) $(BUILDAPP) $(LISP_SRC)
	mkdir -p $(BUILDDIR)/bin
	$(BUILDAPP)      --logfile /tmp/build.log                \
                         $(BUILDAPP_OPTS)                        \
                         --sbcl $(CL)                            \
                         --asdf-path .                           \
                         --asdf-tree $(QLDIR)/local-projects     \
                         --manifest-file $(MANIFEST)             \
                         --asdf-tree $(QLDIR)/dists              \
                         --asdf-path .                           \
                         --load-system $(APP_NAME)               \
                         --load src/hooks.lisp                   \
                         --entry pgloader:main                   \
                         --dynamic-space-size $(DYNSIZE)         \
                         $(COMPRESS_CORE_OPT)                    \
                         --output $@

pgloader: $(PGLOADER) ;

pgloader-standalone:
	$(BUILDAPP)    --require sb-posix                      \
                       --require sb-bsd-sockets                \
                       --require sb-rotate-byte                \
                       --load-system pgloader                  \
                       --entry pgloader:main                   \
                       --dynamic-space-size $(DYNSIZE)         \
                       --compress-core                         \
                       --output $(PGLOADER)

test: $(PGLOADER)
	$(MAKE) PGLOADER=$(realpath $(PGLOADER)) -C test regress

deb:
	# intended for use on a debian system
	mkdir -p $(DEBUILD_ROOT) && rm -rf $(DEBUILD_ROOT)/*
	rsync -Ca --exclude 'build'                      		  \
		  --exclude '.vagrant'                   		  \
		  --exclude 'test/sqlite-chinook.load'   		  \
		  --exclude 'test/sqlite'                		  \
		  --exclude 'test/data/2013_Gaz_113CDs_national.txt'      \
		  --exclude 'test/data/reg2013.dbf'      		  \
		  --exclude 'test/data/sakila-db.zip'    		  \
              ./ $(DEBUILD_ROOT)/
	cd $(DEBUILD_ROOT) && make -f debian/rules orig
	cd $(DEBUILD_ROOT) && debuild -us -uc -sa
	cp -a /tmp/pgloader_* /tmp/cl-pgloader* build/

rpm:
	# intended for use on a CentOS or other RPM based system
	mkdir -p $(DEBUILD_ROOT) && rm -rf $(DEBUILD_ROOT)
	rsync -Ca --exclude=build/* ./ $(DEBUILD_ROOT)/
	cd /tmp && tar czf $(HOME)/rpmbuild/SOURCES/pgloader-$(VERSION).tar.gz pgloader
	cd $(DEBUILD_ROOT) && rpmbuild -ba pgloader.spec
	cp -a $(HOME)/rpmbuild/SRPMS/*rpm build
	cp -a $(HOME)/rpmbuild/RPMS/x86_64/*rpm build

pkg:
	# intended for use on a MacOSX system
	mkdir -p $(DEBUILD_ROOT) && rm -rf $(DEBUILD_ROOT)/*
	mkdir -p $(DEBUILD_ROOT)/usr/local/bin/
	mkdir -p $(DEBUILD_ROOT)/usr/local/share/man/man1/
	cp ./pgloader.1 $(DEBUILD_ROOT)/usr/local/share/man/man1/
	cp ./build/bin/pgloader $(DEBUILD_ROOT)/usr/local/bin/
	pkgbuild --identifier org.tapoueh.pgloader \
	         --root $(DEBUILD_ROOT)            \
	         --version $(VERSION)              \
                 ./build/pgloader-$(VERSION).pkg

latest:
	git archive --format=tar --prefix=pgloader-$(VERSION)/ v$(VERSION) \
        | gzip -9 > $(LATEST)

check: test ;

.PHONY: test pgloader-standalone
