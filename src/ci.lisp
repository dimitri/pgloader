(defpackage #:pgloader-ci
  (:use #:cl)
  (:import-from #:40ants-ci/jobs/linter
                #:linter)
  (:import-from #:40ants-ci/jobs/run-tests
                #:run-tests)
  (:import-from #:40ants-ci/jobs/docs)
  (:import-from #:40ants-ci/workflow
                #:defworkflow))
(in-package pgloader-ci)


(defworkflow ci
  :on-push-to "master"
  :by-cron "0 10 * * 1"
  :on-pull-request t
  :cache t
  :jobs ((linter
          ;; When CI workflow defined in a separate non package-inferred
          ;; package, we have to specify system to test manually.
          :asd-system "pgloader")
         (run-tests
          :os ("ubuntu-latest"
               "macos-latest")
          :quicklisp ("quicklisp"
                      ;; "ultralisp"
                      )
          :lisp ("sbcl-bin"
                 ;; "ccl-bin"
                 ;; "allegro"
                 )


          :asd-system "pgloader"
          :coverage t
          :qlfile "{% ifequal quicklisp_dist \"ultralisp\" %}
                   dist ultralisp http://dist.ultralisp.org
                   {% endifequal %}

                   github mgl-pax svetlyak40wt/mgl-pax :branch mgl-pax-minimal")))
