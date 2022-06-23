(in-package #:pgloader.utils)


(defparameter *ccl-describe-character-encodings*
  ":CP936 [Aliases: :GBK :MS936 :WINDOWS-936]
An 8-bit, variable-length character encoding in which
character code points in the range #x00-#x80 can be encoded in a
single octet; characters with larger code values can be encoded
in 2 bytes.

    Alias :gbk :ms936 :windows-936

:EUC-JP [Aliases: :EUCJP]
An 8-bit, variable-length character encoding in which
character code points in the range #x00-#x7f can be encoded in a
single octet; characters with larger code values can be encoded
in 2 to 3 bytes.

:GB2312 [Aliases: :GB2312-80 :GB2312-1980 :EUC-CN :EUCCN]
An 8-bit, variable-length character encoding in which
character code points in the range #x00-#x80 can be encoded in a
single octet; characters with larger code values can be encoded
in 2 bytes.

    Alias :gb2312-80 :gb2312-1980 :euc-cn :euccn

:ISO-8859-1 [Aliases: :ISO-LATIN-1 :LATIN-1 NIL :ISO_8859-1 :LATIN1 :L1 :IBM819 :CP819 :CSISOLATIN1]
An 8-bit, fixed-width character encoding in which all character
codes map to their Unicode equivalents. Intended to support most
characters used in most Western European languages.

:ISO-8859-10 [Aliases: :ISO-LATIN-6 :LATIN-6 :ISO_8859-10 :LATIN6 :CSISOLATIN6 :ISO-IR-157]
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in Nordic
alphabets.

:ISO-8859-11
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found the  Thai
alphabet.

:ISO-8859-13
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in Baltic
alphabets.

:ISO-8859-14 [Aliases: :ISO-LATIN-8 :LATIN-8 :ISO_8859-14 :ISO-IR-199 :LATIN8 :L8 :ISO-CELTIC]
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in Celtic
languages.

:ISO-8859-15 [Aliases: :ISO-LATIN-9 :LATIN-9 :ISO_8859-15 :LATIN9]
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in Western
European languages (including the Euro sign and some other characters
missing from ISO-8859-1.

:ISO-8859-16 [Aliases: :ISO-LATIN-10 :LATIN-10 :ISO_8859-16 :LATIN10 :L1 :ISO-IR-226]
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in Southeast
European languages.

:ISO-8859-2 [Aliases: :ISO-LATIN-2 :LATIN-2 :ISO_8859-2 :LATIN2 :L2 :CSISOLATIN2]
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in most
languages used in Central/Eastern Europe.

:ISO-8859-3 [Aliases: :ISO-LATIN-3 :LATIN-3 :ISO_8859-3 :LATIN3 :L3 :CSISOLATIN3]
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in most
languages used in Southern Europe.

:ISO-8859-4 [Aliases: :ISO-LATIN-4 :LATIN-4 :ISO_8859-4 :LATIN4 :L4 :CSISOLATIN4]
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in most
languages used in Northern Europe.

:ISO-8859-5 [Aliases: :ISO_8859-5 :CYRILLIC :CSISOLATINCYRILLIC :ISO-IR-144]
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in the
Cyrillic alphabet.

:ISO-8859-6 [Aliases: :ISO_8859-6 :ARABIC :CSISOLATINARABIC :ISO-IR-127]
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in the
Arabic alphabet.

:ISO-8859-7 [Aliases: :ISO_8859-7 :GREEK :GREEK8 :CSISOLATINGREEK :ISO-IR-126 :ELOT_928 :ECMA-118]
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in the
Greek alphabet.

:ISO-8859-8 [Aliases: :ISO_8859-8 :HEBREW :CSISOLATINHEBREW :ISO-IR-138]
An 8-bit, fixed-width character encoding in which codes #x00-#x9f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in the
Hebrew alphabet.

:ISO-8859-9 [Aliases: :ISO-LATIN-5 :LATIN-5 :ISO_8859-9 :LATIN5 :CSISOLATIN5 :ISO-IR-148]
An 8-bit, fixed-width character encoding in which codes #x00-#xcf
map to their Unicode equivalents and other codes map to other Unicode
character values.  Intended to provide most characters found in the
Turkish alphabet.

:MACINTOSH [Aliases: :MACOS-ROMAN :MACOSROMAN :MAC-ROMAN :MACROMAN]
An 8-bit, fixed-width character encoding in which codes #x00-#x7f
map to their Unicode equivalents and other codes map to other Unicode
character values.  Traditionally used on Classic MacOS to encode characters
used in western languages.

:UCS-2
A 16-bit, fixed-length encoding in which characters with
CHAR-CODEs less than #x10000 can be encoded in a single 16-bit word.
The endianness of the encoded data is indicated by the endianness of a
byte-order-mark character (#u+feff) prepended to the data; in the
absence of such a character on input, the data is assumed to be in
big-endian order.

:UCS-2BE
A 16-bit, fixed-length encoding in which characters with
CHAR-CODEs less than #x10000 can be encoded in a single 16-bit
big-endian word. The encoded data is implicitly big-endian;
byte-order-mark characters are not interpreted on input or prepended
to output.

:UCS-2LE
A 16-bit, fixed-length encoding in which characters with
CHAR-CODEs less than #x10000 can be encoded in a single 16-bit
little-endian word. The encoded data is implicitly little-endian;
byte-order-mark characters are not interpreted on input or prepended
to output.

:US-ASCII [Aliases: :CSASCII :CP637 :IBM637 :US :ISO646-US :ASCII :ISO-IR-6]
A 7-bit, fixed-width character encoding in which all character
codes map to their Unicode equivalents.

:UTF-16
A 16-bit, variable-length encoding in which characters with
CHAR-CODEs less than #x10000 can be encoded in a single 16-bit
word and characters with larger codes can be encoded in a
pair of 16-bit words.  The endianness of the encoded data is
indicated by the endianness of a byte-order-mark character (#u+feff)
prepended to the data; in the absence of such a character on input,
the data is assumed to be in big-endian order. Output is written
in native byte-order with a leading byte-order mark.

:UTF-16BE
A 16-bit, variable-length encoding in which characters with
CHAR-CODEs less than #x10000 can be encoded in a single 16-bit
big-endian word and characters with larger codes can be encoded in a
pair of 16-bit big-endian words.  The endianness of the encoded data
is implicit in the encoding; byte-order-mark characters are not
interpreted on input or prepended to output.

:UTF-16LE
A 16-bit, variable-length encoding in which characters with
CHAR-CODEs less than #x10000 can be encoded in a single 16-bit
little-endian word and characters with larger codes can be encoded in
a pair of 16-bit little-endian words.  The endianness of the encoded
data is implicit in the encoding; byte-order-mark characters are not
interpreted on input or prepended to output.

:UTF-32 [Aliases: :UCS-4]
A 32-bit, fixed-length encoding in which all Unicode characters
can be encoded in a single 32-bit word.  The endianness of the encoded
data is indicated by the endianness of a byte-order-mark
character (#u+feff) prepended to the data; in the absence of such a
character on input, input data is assumed to be in big-endian order.
Output is written in native byte order with a leading byte-order
mark.

:UTF-32BE [Aliases: :UCS-4BE]
A 32-bit, fixed-length encoding in which all Unicode characters
encoded in a single 32-bit word. The encoded data is implicitly big-endian;
byte-order-mark characters are not interpreted on input or prepended
to output.

:UTF-32LE [Aliases: :UCS-4LE]
A 32-bit, fixed-length encoding in which all Unicode characters can
encoded in a single 32-bit word. The encoded data is implicitly
little-endian; byte-order-mark characters are not interpreted on input
or prepended to output.

:UTF-8 [Aliases: :MULE-UTF-8]
An 8-bit, variable-length character encoding in which characters
with CHAR-CODEs in the range #x00-#x7f can be encoded in a single
octet; characters with larger code values can be encoded in 2 to 4
bytes.

:WINDOWS-31J [Aliases: :CP932 :CSWINDOWS31J]
An 8-bit, variable-length character encoding in which
character code points in the range #x00-#x7f can be encoded in a
single octet; characters with larger code values can be encoded
in 2 bytes.
")

(defun parse-ccl-encodings-desc-first-line (line)
  "Given a line with :ENCODING [Aliases: :X :Y] return a proper cons."
  (or (cl-ppcre:register-groups-bind (name aliases)
          (":([A-Z0-9-]+).*Aliases: (.*)[]]" line)
        (cons name (mapcar (lambda (alias) (subseq alias 1))
                           (split-sequence:split-sequence #\Space aliases))))
      ;; some of them have no alias
      (cons (subseq line 1) nil)))

(defun parse-ccl-encodings-desc (&optional
                                   (desc *ccl-describe-character-encodings*))
  "Parse the output of the ccl:describe-character-encodings function."
  (with-input-from-string (s desc)
    (loop :for line := (read-line s nil nil)
       :while line
       :when (and line (< 0 (length line)) (char= #\: (aref line 0)))
       :collect (parse-ccl-encodings-desc-first-line line))))

(defun list-encodings-and-aliases ()
  "Return an alist of encoding names supported by the current
  implementation, associated with a list of encoding name aliases for each
  of them."
  (let ((encoding-and-aliases
         #+ccl
          (parse-ccl-encodings-desc (with-output-to-string (*standard-output*)
                                      (ccl:describe-character-encodings)))
         #+sbcl
         (typecase sb-impl::*external-formats*
           (vector
            (loop :for encoding :across sb-impl::*external-formats*
                  :when encoding
                    :collect
                    (mapcar (function string-upcase)
                            (typecase encoding
                              (sb-impl::external-format
                               (slot-value encoding 'sb-impl::names))
                              (list
                               (slot-value (first encoding) 'sb-impl::names))))))
           (hash-table
            (let ((result '()))
            (maphash (lambda (name encoding)
                       (declare (ignore name))
                       (pushnew encoding result))
                     sb-impl::*external-formats*)
            (mapcar (lambda (encoding)
                      (mapcar (function string-upcase)
                              (slot-value encoding 'sb-impl::names)))
                    result))))))
    (sort encoding-and-aliases #'string< :key #'car)))

(defun show-encodings ()
  "List known encodings names and aliases from charsets::*lisp-encodings*."
  (format *standard-output* "Name    ~30TAliases~%")
  (format *standard-output* "--------~30T--------------~%")
  (loop
     :with encodings := (list-encodings-and-aliases)
     :for (name . aliases) :in encodings
     :do (format *standard-output* "~a~30T~{~a~^, ~}~%" name aliases))
  (terpri))

(defun make-external-format (name)
  "Return an object suitable as an external format in the current
  implementation."
  (let ((encoding (intern name "KEYWORD")))
    #+ccl
    (ccl:make-external-format :character-encoding encoding)
    #+sbcl
    encoding))
