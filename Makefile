# pgloader build tool
APP_NAME   = pgloader
VERSION    = 3.6.10

# use either sbcl or ccl
CL	   = sbcl

# default to 16 GB of RAM size in the image
DYNSIZE    = 16384

LISP_SRC   = $(wildcard src/*lisp)         \
             $(wildcard src/monkey/*lisp)  \
             $(wildcard src/utils/*lisp)   \
             $(wildcard src/load/*lisp)    \
             $(wildcard src/parsers/*lisp) \
             $(wildcard src/pg-copy/*lisp) \
             $(wildcard src/pgsql/*lisp)   \
             $(wildcard src/sources/*lisp) \
             pgloader.asd

BUILDDIR   = build
LIBS       = $(BUILDDIR)/libs.stamp
QLDIR      = $(BUILDDIR)/quicklisp
MANIFEST   = $(BUILDDIR)/manifest.ql
LATEST     = $(BUILDDIR)/pgloader-latest.tgz

BUNDLEDIST = 2022-02-20
BUNDLENAME = pgloader-bundle-$(VERSION)
BUNDLEDIR  = $(BUILDDIR)/bundle/$(BUNDLENAME)
BUNDLE     = $(BUILDDIR)/$(BUNDLENAME).tgz
BUNDLETESTD= $(BUILDDIR)/bundle/test

ifeq ($(OS),Windows_NT)
EXE           = .exe
COMPRESS_CORE = no
DYNSIZE       = 1024		# support for windows 32 bits
else
EXE =
endif

PGLOADER   = $(BUILDDIR)/bin/$(APP_NAME)$(EXE)
BUILDAPP_CCL  = $(BUILDDIR)/bin/buildapp.ccl$(EXE)
BUILDAPP_SBCL = $(BUILDDIR)/bin/buildapp.sbcl$(EXE)

ifeq ($(CL),sbcl)
BUILDAPP      = $(BUILDAPP_SBCL)
BUILDAPP_OPTS = --require sb-posix                      \
                --require sb-bsd-sockets                \
                --require sb-rotate-byte
CL_OPTS    = --noinform --no-sysinit --no-userinit
else
BUILDAPP   = $(BUILDAPP_CCL)
CL_OPTS    = --no-init
endif

ifeq ($(CL),sbcl)
COMPRESS_CORE ?= $(shell $(CL) --noinform \
                               --quit     \
                               --eval '(when (member :sb-core-compression cl:*features*) (write-string "yes"))')

endif

# note: on Windows_NT, we never core-compress; see above.
ifeq ($(COMPRESS_CORE),yes)
COMPRESS_CORE_OPT = --compress-core
endif

DEBUILD_ROOT = /tmp/pgloader

all: $(PGLOADER)

clean:
	rm -rf $(LIBS) $(QLDIR) $(MANIFEST) $(BUILDAPP) $(PGLOADER) \
		buildapp.log build/bundle/* build/pgloader-bundle* build/quicklisp.lisp docs/_build
	$(MAKE) -C test clean

$(QLDIR)/local-projects/qmynd:
	git clone --depth 1 https://github.com/qitab/qmynd.git $@

$(QLDIR)/local-projects/cl-ixf:
	git clone --depth 1 https://github.com/dimitri/cl-ixf.git $@

$(QLDIR)/local-projects/cl-db3:
	git clone --depth 1 https://github.com/dimitri/cl-db3.git $@

$(QLDIR)/local-projects/cl-csv:
	git clone --depth 1 https://github.com/AccelerationNet/cl-csv.git $@

$(QLDIR)/setup.lisp:
	mkdir -p $(BUILDDIR)
	curl -o $(BUILDDIR)/quicklisp.lisp http://beta.quicklisp.org/quicklisp.lisp
	$(CL) $(CL_OPTS) --load $(BUILDDIR)/quicklisp.lisp                        \
             --load src/getenv.lisp                                               \
             --eval '(quicklisp-quickstart:install :path "$(BUILDDIR)/quicklisp" :proxy (getenv "http_proxy"))' \
             --eval '(quit)'

quicklisp: $(QLDIR)/setup.lisp ;

clones: $(QLDIR)/local-projects/cl-ixf \
        $(QLDIR)/local-projects/cl-db3 \
        $(QLDIR)/local-projects/cl-csv \
        $(QLDIR)/local-projects/qmynd ;

$(LIBS): $(QLDIR)/setup.lisp
	$(CL) $(CL_OPTS) --load $(QLDIR)/setup.lisp                   \
             --eval '(push :pgloader-image *features*)'               \
             --eval '(setf *print-circle* t *print-pretty* t)'        \
             --eval '(push "$(PWD)/" ql:*local-project-directories*)' \
             --eval '(ql:quickload "pgloader")'                       \
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
                         --load-system cffi                      \
                         --load-system cl+ssl                    \
                         --load-system mssql                     \
                         --load src/hooks.lisp                   \
                         --load-system $(APP_NAME)               \
                         --entry pgloader:main                   \
                         --dynamic-space-size $(DYNSIZE)         \
                         $(COMPRESS_CORE_OPT)                    \
                         --output $@.tmp
	# that's ugly, but necessary when building on Windows :(
	mv $@.tmp $@

pgloader: $(PGLOADER) ;

pgloader-standalone:
	$(BUILDAPP)    $(BUILDAPP_OPTS)                        \
                       --sbcl $(CL)                            \
                       --load-system $(APP_NAME)               \
                       --load src/hooks.lisp                   \
                       --entry pgloader:main                   \
                       --dynamic-space-size $(DYNSIZE)         \
                       $(COMPRESS_CORE_OPT)                    \
                       --output $(PGLOADER)
test: $(PGLOADER)
	$(MAKE) PGLOADER=$(realpath $(PGLOADER)) CL=$(CL) -C test regress

save: ./src/save.lisp $(LISP_SRC)
	$(CL) $(CL_OPTS) --load ./src/save.lisp

check-saved:
	$(MAKE) PGLOADER=$(realpath $(PGLOADER)) CL=$(CL) -C test regress

clean-bundle:
	rm -rf $(BUNDLEDIR)
	rm -rf $(BUNDLETESTD)/$(BUNDLENAME)/*

$(BUNDLETESTD):
	mkdir -p $@

$(BUNDLEDIR): quicklisp
	mkdir -p $@
	$(CL) $(CL_OPTS) --load $(QLDIR)/setup.lisp      \
             --eval '(defvar *bundle-dir* "$@")'         \
             --eval '(defvar *pwd* "$(PWD)/")'           \
             --eval '(defvar *ql-dist* "$(BUNDLEDIST)")' \
             --load bundle/ql.lisp

$(BUNDLEDIR)/version.sexp: $(BUNDLEDIR)
	echo "\"$(VERSION)\"" > $@

$(BUNDLE): $(BUNDLEDIR) $(BUNDLEDIR)/version.sexp
	cp bundle/README.md $(BUNDLEDIR)
	cp bundle/save.lisp $(BUNDLEDIR)
	sed -e s/%VERSION%/$(VERSION)/ < bundle/Makefile > $(BUNDLEDIR)/Makefile
	git archive --format=tar --prefix=pgloader-$(VERSION)/ master \
	     | tar -C $(BUNDLEDIR)/local-projects/ -xf -
	make QLDIR=$(BUNDLEDIR) clones
	tar -C build/bundle 		    \
            --exclude bin   		    \
            --exclude test/sqlite           \
            -czf $@ $(BUNDLENAME)

bundle: clean-bundle $(BUNDLE) $(BUNDLETESTD)
	tar -C $(BUNDLETESTD) -xf $(BUNDLE)
	make -C $(BUNDLETESTD)/$(BUNDLENAME)
	$(BUNDLETESTD)/$(BUNDLENAME)/bin/pgloader --version

test-bundle:
	$(MAKE) -C $(BUNDLEDIR) test


deb:
	# intended for use on a debian system
	mkdir -p $(DEBUILD_ROOT) && rm -rf $(DEBUILD_ROOT)/*
	rsync -Ca --exclude 'build'                      		  \
		  --exclude '.vagrant'                   		  \
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

.PHONY: test pgloader-standalone docs bundle
