# pgloader build tool
APP_NAME   = pgloader

SBCL	   = sbcl
SBCL_OPTS  = --no-sysinit --no-userinit

COMPRESS_CORE ?= yes

ifeq ($(COMPRESS_CORE),yes)
COMPRESS_CORE_OPT = --compress-core
else
COMPRESS_CORE_OPT = 
endif

BUILDDIR   = build
LIBS       = $(BUILDDIR)/libs.stamp
BUILDAPP   = $(BUILDDIR)/bin/buildapp
MANIFEST   = $(BUILDDIR)/manifest.ql
PGLOADER   = $(BUILDDIR)/bin/$(APP_NAME)
QLDIR      = $(BUILDDIR)/quicklisp

DEBUILD_ROOT = /tmp/pgloader

all: $(PGLOADER)

docs:
	ronn -roff pgloader.1.md

$(QLDIR)/local-projects/qmynd:
	git clone https://github.com/qitab/qmynd.git $@

qmynd: $(QLDIR)/local-projects/qmynd
	cd $< && git pull

$(QLDIR)/setup.lisp:
	mkdir -p $(BUILDDIR)
	curl -o $(BUILDDIR)/quicklisp.lisp http://beta.quicklisp.org/quicklisp.lisp
	$(SBCL) $(SBCL_OPTS) --load $(BUILDDIR)/quicklisp.lisp                     \
             --eval '(quicklisp-quickstart:install :path "$(BUILDDIR)/quicklisp")' \
             --eval '(quit)'

quicklisp: $(QLDIR)/setup.lisp ;

$(LIBS): quicklisp qmynd
	$(SBCL) $(SBCL_OPTS) --load $(QLDIR)/setup.lisp             \
             --eval '(ql:quickload "pgloader")'                     \
             --eval '(quit)'
	touch $@

libs: $(LIBS) ;

$(MANIFEST): libs
	$(SBCL) $(SBCL_OPTS) --load $(QLDIR)/setup.lisp            \
             --eval '(ql:write-asdf-manifest-file "$(MANIFEST)")'  \
             --eval '(quit)'

manifest: $(MANIFEST) ;

$(BUILDAPP): quicklisp
	mkdir -p $(BUILDDIR)/bin
	$(SBCL) $(SBCL_OPTS) --load $(QLDIR)/setup.lisp           \
             --eval '(ql:quickload "buildapp")'                   \
             --eval '(buildapp:build-buildapp "$(BUILDAPP)")'     \
             --eval '(quit)'

buildapp: $(BUILDAPP) ;

$(PGLOADER): manifest buildapp
	mkdir -p $(BUILDDIR)/bin
	$(BUILDAPP)      --logfile /tmp/build.log                \
                         --require sb-posix                      \
                         --require sb-bsd-sockets                \
                         --require sb-rotate-byte                \
                         --asdf-path .                           \
                         --asdf-tree $(QLDIR)/local-projects     \
                         --manifest-file $(MANIFEST)             \
                         --asdf-tree $(QLDIR)/dists              \
                         --asdf-path .                           \
                         --load-system $(APP_NAME)               \
                         --load src/hooks.lisp                   \
                         --entry pgloader:main                   \
                         --dynamic-space-size 4096               \
                         $(COMPRESS_CORE_OPT)                    \
                         --output $@

pgloader: $(PGLOADER) ;

pgloader-standalone:
	$(BUILDAPP)    --require sb-posix                      \
                       --require sb-bsd-sockets                \
                       --require sb-rotate-byte                \
                       --load-system pgloader                  \
                       --entry pgloader:main                   \
                       --dynamic-space-size 4096               \
                       --compress-core                         \
                       --output $(PGLOADER)

test:
	$(MAKE) PGLOADER=$(realpath $(PGLOADER)) -C test all

deb: docs
	# intended for use on a debian system
	mkdir -p $(DEBUILD_ROOT) && rm -rf $(DEBUILD_ROOT)/*
	rsync -Ca --exclude=build/* ./ $(DEBUILD_ROOT)/
	cd $(DEBUILD_ROOT) && make -f debian/rules orig
	cd $(DEBUILD_ROOT) && debuild -us -uc -sa
	cp -a /tmp/pgloader_* build/

rpm:
	# intended for use on a CentOS or other RPM based system
	mkdir -p $(DEBUILD_ROOT) && rm -rf $(DEBUILD_ROOT)/*
	rsync -Ca --exclude=build/* ./ $(DEBUILD_ROOT)/
	cd /tmp && tar czf $(HOME)/rpmbuild/SOURCES/pgloader-3.0.98.tar.gz pgloader
	cd $(DEBUILD_ROOT) && rpmbuild -ba pgloader.spec
	cp -a $(HOME)/rpmbuild/SRPMS/*rpm build
	cp -a $(HOME)/rpmbuild/RPMS/x86_64/*rpm build

check: test ;

.PHONY: test pgloader-standalone
