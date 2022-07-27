# pgloader build tool for bundle tarball
# only supports SBCL
CL = sbcl

APP_NAME   = pgloader
VERSION    = %VERSION%

ifeq ($(OS),Windows_NT)
EXE           = .exe
COMPRESS_CORE = no
DYNSIZE       = 1024		# support for windows 32 bits
else
DYNSIZE       = 16384
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

$(PGLOADER): $(BUILDAPP)
	$(BUILDAPP)      --logfile /tmp/pgloader-bundle-build.log     \
                         $(BUILDAPP_OPTS)                             \
                         --sbcl $(CL)                                 \
                         --asdf-tree .                                \
                         --load-system cffi                           \
                         --load-system cl+ssl                         \
                         --load-system mssql                          \
                         --load $(SRCDIR)/src/hooks.lisp              \
                         --load-system $(APP_NAME)                    \
                         --eval '(setf pgloader.params::*version-string* "$(VERSION)")' \
                         --entry pgloader:main                        \
                         --dynamic-space-size $(DYNSIZE)              \
                         $(COMPRESS_CORE_OPT)                         \
                         --output $@.tmp
	# that's ugly, but necessary when building on Windows :(
	mv $@.tmp $@

test: $(PGLOADER)
	$(MAKE) PGLOADER=$(realpath $(PGLOADER)) -C $(SRCDIR)/test regress

save:
	sbcl --no-userinit --load ./save.lisp

check: test ;
