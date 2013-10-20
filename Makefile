# pgloader build tools

POMO_PATCH = $(realpath patches/postmodern-send-copy-done.patch)
ASDF_CONFD = ~/.config/common-lisp/source-registry.conf.d
ASDF_CONF  = $(ASDF_CONFD)/projects.conf

docs:
	pandoc pgloader.1.md -o pgloader.1
	pandoc pgloader.1.md -o pgloader.html
	pandoc pgloader.1.md -o pgloader.pdf

~/quicklisp/local-projects/Postmodern:
	git clone https://github.com/marijnh/Postmodern.git $@
	cd ~/quicklisp/local-projects/Postmodern/ && patch -p1 < $(POMO_PATCH)

postmodern: ~/quicklisp/local-projects/Postmodern ;

~/quicklisp/local-projects/cl-csv:
	git clone -b empty-strings-and-nil https://github.com/dimitri/cl-csv.git $@

cl-csv: ~/quicklisp/local-projects/cl-csv ;

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

libs: quicklisp $(ASDF_CONF) postmodern cl-csv
	# Quicklisp Install needed Common Lisp libs
	sbcl --load ~/quicklisp/setup.lisp                             \
             --eval '(ql:quickload "pgloader")'                        \
             --eval '(quit)'

./build/manifest.ql: libs
	sbcl --load ~/quicklisp/setup.lisp                                 \
             --eval '(ql:write-asdf-manifest-file "./build/manifest.ql")'  \
             --eval '(quit)'

./build/buildapp: quicklisp
	sbcl --load ~/quicklisp/setup.lisp                          \
             --eval '(ql:quickload "buildapp")'                     \
             --eval '(buildapp:build-buildapp "./build/buildapp")'  \
             --eval '(quit)'

./build/pgloader.exe: ./build/buildapp ./build/manifest.ql
	./build/buildapp --logfile /tmp/build.log                \
                         --asdf-tree ~/quicklisp/local-projects  \
                         --manifest-file ./build/manifest.ql     \
                         --asdf-tree ~/quicklisp/dists           \
                         --asdf-path .                           \
                         --load-system pgloader                  \
                         --entry pgloader:main                   \
                         --dynamic-space-size 4096               \
                         --compress-core                         \
                         --output build/pgloader.exe

test: ./build/pgloader.exe
	$(MAKE) PGLOADER=$(realpath ./build/pgloader.exe) -C test all

check: test ;
