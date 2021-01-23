;;; Commentary:
;;
;; GNU Guix development package. It was inspired by Nyxt's
;; build-scripts/guix.scm https://github.com/atlas-engineer/nyxt
;;
;;   guix package --install-from-file=bootstrap-guix.scm
;;
;; To use as the basis for a development environment, run:
;;
;;   guix environment --pure --load=bootstrap-guix.scm --ad-hoc sbcl unzip sqlite curl freetds
;;
;; To start in a container, run:
;;
;;   guix environment --no-grafts --load=bootstrap-guix.scm --container --network --share=$(pwd)=/pgloader --ad-hoc sbcl unzip sqlite curl freetds
;;
;; Then in the container environment:
;;
;;   cd /pgloader
;;   make pgloader
;;   ./build/bin/pgloader --help
;;
;;; Code:

(use-modules ((guix build utils) #:select (with-directory-excursion))
             (gnu packages lisp)
             (gnu packages lisp-xyz)
             (gnu packages pkg-config)
             (gnu packages version-control)
             (gnu packages)
             (guix build lisp-utils)
             (guix build-system asdf)
             (guix build-system gnu)
             (guix gexp)
             (guix git-download)
             (guix packages)
             (ice-9 match)
             (ice-9 popen)
             (ice-9 rdelim)
             (srfi srfi-1)
             (srfi srfi-26)
             ((guix licenses) #:prefix license:))

(define %source-dir (dirname (current-filename)))

(define git-file?
  (let* ((pipe (with-directory-excursion %source-dir
                 (open-pipe* OPEN_READ "git" "ls-files")))
         (files (let loop ((lines '()))
                  (match (read-line pipe)
                    ((? eof-object?)
                     (reverse lines))
                    (line
                     (loop (cons line lines))))))
         (status (close-pipe pipe)))
    (lambda (file stat)
      (match (stat:type stat)
        ('directory
         #t)
        ((or 'regular 'symlink)
         (any (cut string-suffix? <> file) files))
        (_
         #f)))))

(define (pgloader-git-version)
  (let* ((pipe (with-directory-excursion %source-dir
                 (open-pipe* OPEN_READ "git" "describe" "--always" "--tags")))
         (version (read-line pipe)))
    (close-pipe pipe)
    version))

(define-public pgloader
  (package
    (name "pgloader")
    (version (pgloader-git-version))
    (source (local-file %source-dir #:recursive? #t #:select? git-file?))
    (build-system gnu-build-system)
    (native-inputs
     `(("buildapp" ,buildapp)
       ("sbcl" ,sbcl)))
    (inputs
     `(("alexandria" ,sbcl-alexandria)
       ("cl-abnf" ,sbcl-cl-abnf)
       ("cl-base64" ,sbcl-cl-base64)
       ("cl-csv" ,sbcl-cl-csv)
       ("cl-fad" ,sbcl-cl-fad)
       ("cl-log" ,sbcl-cl-log)
       ("cl-markdown" ,sbcl-cl-markdown)
       ("cl-mustache" ,sbcl-cl-mustache)
       ("cl-ppcre" ,sbcl-cl-ppcre)
       ("cl-sqlite" ,sbcl-cl-sqlite)
       ("closer-mop" ,sbcl-closer-mop)
       ("command-line-arguments" ,sbcl-command-line-arguments)
       ("db3" ,sbcl-db3)
       ("drakma" ,sbcl-drakma)
       ("esrap" ,sbcl-esrap)
       ("flexi-streams" ,sbcl-flexi-streams)
       ("ixf" ,sbcl-ixf)
       ("local-time" ,sbcl-local-time)
       ("lparallel" ,sbcl-lparallel)
       ("metabang-bind" ,sbcl-metabang-bind)
       ("mssql" ,sbcl-mssql)
       ("postmodern" ,sbcl-postmodern)
       ("py-configparser" ,sbcl-py-configparser)
       ("qmynd" ,sbcl-qmynd)
       ("quri" ,sbcl-quri)
       ("split-sequence" ,sbcl-split-sequence)
       ("trivial-backtrace" ,sbcl-trivial-backtrace)
       ("usocket" ,sbcl-usocket)
       ("uuid" ,sbcl-uuid)
       ("yason" ,sbcl-yason)
       ("zs3" ,sbcl-zs3)))
    (arguments
     `(#:tests? #f
       #:strip-binaries? #f
       #:make-flags
       (list "pgloader-standalone" "BUILDAPP_SBCL=buildapp")
       #:phases
       (modify-phases %standard-phases
         (delete 'configure)
         (add-after 'unpack 'set-home
           (lambda _
             (setenv "HOME" "/tmp")
             #t))
         (add-after 'unpack 'patch-Makefile
           (lambda _
             (substitute* "Makefile"
               (("--sbcl.*") "--sbcl $(CL) --asdf-path . \\\n"))
             #t))
         (replace 'install
           (lambda* (#:key outputs #:allow-other-keys)
             (let ((bin (string-append (assoc-ref outputs "out") "/bin")))
               (mkdir-p bin)
               (install-file "build/bin/pgloader"  bin))
             #t)))))
    (home-page "https://pgloader.io/")
    (synopsis "Migration to PostgreSQL tool")
    (description
     "It allows to migrate from CSV, DB3, iXF, SQLite, MS-SQL, MySQL to
PostgreSQL.")
    (license license:expat)))

pgloader
