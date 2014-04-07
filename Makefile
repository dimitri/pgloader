# pgloader build tool
ASDF_CONFD = ~/.config/common-lisp/source-registry.conf.d
ASDF_CONF  = $(ASDF_CONFD)/pgloader.conf

LIBS       = build/libs.stamp
BUILDAPP   = build/buildapp
MANIFEST   = build/manifest.ql
PGLOADER   = build/pgloader.exe

DEBUILD_ROOT = /tmp/pgloader

all: $(PGLOADER)

docs:
	ronn -roff pgloader.1.md

~/quicklisp/local-projects/qmynd:
	git clone https://github.com/qitab/qmynd.git $@

qmynd: ~/quicklisp/local-projects/qmynd ;

~/quicklisp/setup.lisp:
	curl -o ~/quicklisp.lisp http://beta.quicklisp.org/quicklisp.lisp
	sbcl --load ~/quicklisp.lisp                  \
             --eval '(quicklisp-quickstart:install)'  \
             --eval '(quit)'

quicklisp: ~/quicklisp/setup.lisp ;

$(ASDF_CONF):
	mkdir -p $(ASDF_CONFD)
	echo "(:tree \"`pwd`\")" > $@

asdf-config: $(ASDF_CONF) ;

$(LIBS): quicklisp $(ASDF_CONF) qmynd
	sbcl --load ~/quicklisp/setup.lisp                             \
             --eval '(ql:quickload "pgloader")'                        \
             --eval '(quit)'
	touch $@

libs: $(LIBS) ;

$(MANIFEST): libs
	sbcl --load ~/quicklisp/setup.lisp                                 \
             --eval '(ql:write-asdf-manifest-file "./build/manifest.ql")'  \
             --eval '(quit)'

manifest: $(MANIFEST) ;

$(BUILDAPP): quicklisp
	sbcl --load ~/quicklisp/setup.lisp                          \
             --eval '(ql:quickload "buildapp")'                     \
             --eval '(buildapp:build-buildapp "./build/buildapp")'  \
             --eval '(quit)'

buildapp: $(BUILDAPP) ;

$(PGLOADER): manifest buildapp
	./build/buildapp --logfile /tmp/build.log                \
                         --require sb-posix                      \
                         --require sb-bsd-sockets                \
                         --require sb-rotate-byte                \
                         --asdf-tree ~/quicklisp/local-projects  \
                         --manifest-file ./build/manifest.ql     \
                         --asdf-tree ~/quicklisp/dists           \
                         --asdf-path .                           \
                         --load-system pgloader                  \
                         --entry pgloader:main                   \
                         --dynamic-space-size 4096               \
                         --compress-core                         \
                         --output $@

pgloader: $(PGLOADER) ;

pgloader-standalone:
	buildapp             --require sb-posix                      \
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
