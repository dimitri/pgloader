The pgloader parser reads the command language (a pgloader specific DSL) and
produces lisp code as its output. The lisp code is then compiled at run-time
and executed.

The generated lisp-code uses the generic API defined in src/sources.lisp and
creates objects specifics to the kind of source that is being loaded.

It is possible to see the generated code for study or debug:
               
    PARSER> (pprint
             (with-monitor ()
               (parse-commands-from-file
                "/Users/dim/dev/pgloader/test/fixed.load")))
               
