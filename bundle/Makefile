# pgloader build tool for bundle tarball
# only supports SBCL
CL = sbcl

APP_NAME   = pgloader
VERSION    = 3.3.0.50

ifeq ($(OS),Windows_NT)
EXE           = .exe
COMPRESS_CORE = no
DYNSIZE       = 1024		# support for windows 32 bits
else
DYNSIZE       = 4096
EXE =
endif

BUILDDIR   = bin
BUILDAPP   = $(BUILDDIR)/buildapp$(EXE)
PGLOADER   = ./bin/pgloader

SRCDIR = local-projects/pgloader-$(VERSION)

BUILDAPP_OPTS = --require sb-posix                      \
                --require sb-bsd-sockets                \
                --require sb-rotate-byte
CL_OPTS    = --noinform --no-sysinit --no-userinit

COMPRESS_CORE ?= $(shell $(CL) --noinform \
                               --quit     \
                               --eval '(when (member :sb-core-compression cl:*features*) (write-string "yes"))')

ifeq ($(COMPRESS_CORE),yes)
COMPRESS_CORE_OPT = --compress-core
endif

pgloader: $(PGLOADER) ;
buildapp: $(BUILDAPP) ;

$(BUILDAPP):
	mkdir -p $(BUILDDIR)
	$(CL) $(CL_OPTS) --load bundle.lisp                           \
             --eval '(asdf:load-system :buildapp)'                    \
             --eval '(buildapp:build-buildapp "$@")'                  \
             --eval '(quit)'

$(PGLOADER): $(BUILDAPP) clones
	$(BUILDAPP)      --logfile /tmp/pgloader-bundle-build.log     \
                         $(BUILDAPP_OPTS)                             \
                         --sbcl $(CL)                                 \
                         --asdf-tree .                                \
                         --load-system $(APP_NAME)                    \
                         --eval '(setf pgloader.params::*version-string* "$(VERSION)")' \
                         --load $(SRCDIR)/src/hooks.lisp              \
                         --entry pgloader:main                        \
                         --dynamic-space-size $(DYNSIZE)              \
                         $(COMPRESS_CORE_OPT)                         \
                         --output $@.tmp
	# that's ugly, but necessary when building on Windows :(
	mv $@.tmp $@

#
# Provide the current versions of some packages, not the Quicklisp one:
#
local-projects/qmynd:
	git clone https://github.com/qitab/qmynd.git $@

local-projects/cl-ixf:
	git clone https://github.com/dimitri/cl-ixf.git $@

local-projects/cl-db3:
	git clone https://github.com/dimitri/cl-db3.git $@

local-projects/cl-csv:
	git clone https://github.com/AccelerationNet/cl-csv.git $@

local-projects/esrap:
	git clone https://github.com/scymtym/esrap.git $@

clones: local-projects/cl-ixf \
        local-projects/cl-db3 \
        local-projects/cl-csv \
        local-projects/qmynd  \
        local-projects/esrap ;

test: $(PGLOADER)
	$(MAKE) PGLOADER=$(realpath $(PGLOADER)) -C $(SRCDIR)/test regress

check: test ;
