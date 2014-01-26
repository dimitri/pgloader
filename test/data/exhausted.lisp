;;; Test cases for issue https://github.com/dimitri/pgloader/issues/16
;;;
;;; Table already created as:

#|
CREATE TABLE IF NOT EXISTS `document` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `document_template_id` int(10) unsigned NOT NULL,
  `brand_id` int(10) unsigned DEFAULT NULL,
  `logo` char(1) NOT NULL DEFAULT '0',
  `footer` char(1) NOT NULL DEFAULT '0',
  `pages` char(1) NOT NULL DEFAULT '0',
  `content` longtext NOT NULL,
  `meta` text,
  `status` char(1) NOT NULL DEFAULT '1',
  `date_created` datetime NOT NULL,
  `user_id` varchar(128) NOT NULL,
  `region` varchar(32) DEFAULT NULL,
  `foreign_id` int(10) unsigned NOT NULL,
  `date_sent` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `region` (`region`,`foreign_id`) USING BTREE,
  KEY `document_template_id` (`document_template_id`) USING BTREE
) ENGINE=InnoDB  DEFAULT CHARSET=utf8;
|#

(defpackage #:pgloader.test.exhausted
  (:use #:cl #:pgloader.params #:pgloader.mysql)
  (:export #:produce-data))

(in-package #:pgloader.test.exhausted)

(defvar *string* (make-string 237563 :initial-element #\a)
  "A long string to reproduce heap exhaustion.")

(defun produce-data (&key
                       (*myconn-host* *myconn-host*)
                       (*myconn-port* *myconn-port*)
                       (*myconn-user* *myconn-user*)
                       (*myconn-pass* *myconn-pass*)
                       (dbname "exhausted")
                       (rows 5000))
  "Insert data to reproduce the test case."
  (with-mysql-connection (dbname)
    (loop repeat rows
       do (pgloader.mysql::mysql-query (format nil "
INSERT INTO `document` (`document_template_id`,
                        `brand_id`,
                        `logo`,
                        `footer`,
                        `pages`,
                        `content`,
                        `meta`,
                        `status`,
                        `date_created`,
                        `user_id`,
                        `region`,
                        `foreign_id`,
                        `date_sent`)
     VALUES (20, 21, '0', '0', '0', '~a',
             'a:2:{s:7:\"comment\";s:0:\"\";s:4:\"date\";s:10:\"1372975200\";}',
              '1', '2013-06-21 13:04:46', 'cjumeaux', 'dossier', 104027,
              '2013-06-21 13:04:46');" *string*)))))
