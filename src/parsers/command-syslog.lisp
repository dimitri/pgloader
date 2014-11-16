;;;
;;; LOAD MESSAGES FROM syslog
;;;

(in-package #:pgloader.parser)

#|
    LOAD MESSAGES FROM syslog://localhost:10514/

        WHEN MATCHES rsyslog-msg IN apache
         REGISTERING timestamp, ip, rest
        INTO postgresql://localhost/db?logs.apache
         SET guc_1 = 'value', guc_2 = 'other value'

        WHEN MATCHES rsyslog-msg IN others
         REGISTERING timestamp, app-name, data
        INTO postgresql://localhost/db?logs.others
         SET guc_1 = 'value', guc_2 = 'other value'

        WITH apache = rsyslog
             DATA   = IP REST
             IP     = 1*3DIGIT \".\" 1*3DIGIT \".\"1*3DIGIT \".\"1*3DIGIT
             REST   = ~/.*/

        WITH others = rsyslog;
|#
(defrule rule-name (and (alpha-char-p character)
			(+ (abnf::rule-name-character-p character)))
  (:lambda (name)
    (text name)))

(defrule rules (* (not (or kw-registering
			   kw-with
			   kw-when
			   kw-set
			   end-of-command)))
  (:text t))

(defrule rule-name-list (and rule-name
			     (+ (and "," ignore-whitespace rule-name)))
  (:lambda (list)
    (destructuring-bind (name names) list
      (list* name (mapcar (lambda (x)
			    (destructuring-bind (c w n) x
			      (declare (ignore c w))
			      n)) names)))))

(defrule syslog-grammar (and kw-with rule-name equal-sign rule-name rules)
  (:lambda (grammar)
    (bind (((_ top-level _ gram abnf) grammar)
           (default-abnf-grammars
	      `(("rsyslog" . ,abnf:*abnf-rsyslog*)
		("syslog"  . ,abnf:*abnf-rfc5424-syslog-protocol*)
		("syslog-draft-15" . ,abnf:*abnf-rfc-syslog-draft-15*)))
           (grammar (cdr (assoc gram default-abnf-grammars :test #'string=))))
      (cons top-level
	      (concatenate 'string
			   abnf
			   '(#\Newline #\Newline)
			   grammar)))))

(defrule register-groups (and kw-registering rule-name-list)
  (:lambda (groups)
    (bind (((_ rule-names) groups)) rule-names)))

(defrule syslog-match (and kw-when
			   kw-matches rule-name kw-in rule-name
			   register-groups
			   target
			   (? gucs))
  (:lambda (matches)
    (bind (((_ _ top-level _ rule-name groups target gucs) matches))
      (list :target target
	    :gucs gucs
	    :top-level top-level
	    :grammar rule-name
	    :groups groups))))

(defrule syslog-connection-uri (and dsn-prefix dsn-hostname (? "/"))
  (:lambda (syslog)
    (bind (((prefix host-port _)  syslog)
           ((&key type host port) (append prefix host-port)))
      (list :type type
            :host host
            :port port))))

(defrule syslog-source (and ignore-whitespace
			      kw-load kw-messages kw-from
			      syslog-connection-uri)
  (:lambda (source)
    (bind (((_ _ _ _ uri) source)) uri)))

(defrule load-syslog-messages (and syslog-source
				   (+ syslog-match)
				   (+ syslog-grammar))
  (:lambda (syslog)
    (bind (((syslog-server matches grammars) syslog)
           ((&key ((:host syslog-host))
                  ((:port syslog-port))
                  &allow-other-keys)         syslog-server)
           (scanners
            (loop
               for match in matches
               collect (destructuring-bind (&key target
                                                 gucs
                                                 top-level
                                                 grammar
                                                 groups)
                           match
                         (list :target target
                               :gucs gucs
                               :parser (abnf:parse-abnf-grammar
                                        (cdr (assoc grammar grammars
                                                    :test #'string=))
                                        top-level
                                        :registering-rules groups)
                               :groups groups)))))
      `(lambda ()
         (let ((scanners ',scanners))
           (pgloader.syslog:stream-messages :host ,syslog-host
                                            :port ,syslog-port
                                            :scanners scanners))))))
