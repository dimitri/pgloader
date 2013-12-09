# pgloader build tool
ASDF_CONFD = ~/.config/common-lisp/source-registry.conf.d
ASDF_CONF  = $(ASDF_CONFD)/projects.conf

LIBS       = build/libs.stamp
BUILDAPP   = build/buildapp
MANIFEST   = build/manifest.ql
PGLOADER   = build/pgloader.exe

DEBUILD_ROOT = /tmp/pgloader

all: $(PGLOADER)

docs:
	pandoc pgloader.1.md -o pgloader.1

~/quicklisp/local-projects/qmynd:
	git clone https://github.com/qitab/qmynd.git $@

~/quicklisp/local-projects/cl-abnf:
	git clone https://github.com/dimitri/cl-abnf.git $@

~/quicklisp/local-projects/Postmodern:
	git clone https://github.com/marijnh/Postmodern.git $@

~/quicklisp/local-projects/cl-csv:
	git clone -b empty-strings-and-nil https://github.com/dimitri/cl-csv.git $@

qmynd: ~/quicklisp/local-projects/qmynd ;
cl-abnf: ~/quicklisp/local-projects/cl-abnf ;
cl-csv: ~/quicklisp/local-projects/cl-csv ;
postmodern: ~/quicklisp/local-projects/Postmodern ;

~/quicklisp/setup.lisp:
	curl -o ~/quicklisp.lisp http://beta.quicklisp.org/quicklisp.lisp
	sbcl --load ~/quicklisp.lisp                  \
             --eval '(quicklisp-quickstart:install)'  \
             --eval '(quit)'

quicklisp: ~/quicklisp/setup.lisp ;

$(ASDF_CONF):
	mkdir -p $(ASDF_CONFD)
	echo '(:tree "/vagrant")' > $@

asdf-config: $(ASDF_CONF) ;

$(LIBS): quicklisp $(ASDF_CONF) cl-abnf postmodern cl-csv qmynd
	sbcl --load ~/quicklisp/setup.lisp                             \
             --eval '(ql:quickload "pgloader")'                        \
             --eval '(quit)'
	touch $@

libs: $(LIBS) ;

$(MANIFEST): libs
	sbcl --load ~/quicklisp/setup.lisp                                 \
             --eval '(ql:write-asdf-manifest-file "./build/manifest.ql")'  \
             --eval '(quit)'

$(BUILDAPP): quicklisp
	sbcl --load ~/quicklisp/setup.lisp                          \
             --eval '(ql:quickload "buildapp")'                     \
             --eval '(buildapp:build-buildapp "./build/buildapp")'  \
             --eval '(quit)'

$(PGLOADER): $(MANIFEST) $(BUILDAPP)
	./build/buildapp --logfile /tmp/build.log                \
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

new-vm:
	vagrant destroy -f
	vagrant up

vm:
	 vagrant up

vm-build: vm
	vagrant ssh -c "make -C /vagrant"

test:
	$(MAKE) PGLOADER=$(realpath $(PGLOADER)) -C test all

deb:
	mkdir -p $(DEBUILD_ROOT) && rm -rf $(DEBUILD_ROOT)/*
	rsync -Ca --exclude=build/* ./ $(DEBUILD_ROOT)/
	cd $(DEBUILD_ROOT) && make -f debian/rules orig
	cd $(DEBUILD_ROOT) && debuild -us -uc -sa
	cp -a /tmp/pgloader_* build/

check: test ;

.PHONY: test
