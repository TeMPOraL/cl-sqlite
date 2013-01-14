(defpackage :sqlite
  (:use :cl :iter)
  (:export :sqlite-error
           :sqlite-constraint-error
           :sqlite-error-db-handle
           :sqlite-error-code
           :sqlite-error-message
           :sqlite-error-sql
           :sqlite-handle
           :connect
           :set-busy-timeout
           :disconnect
           :sqlite-statement
           :prepare-statement
           :finalize-statement
           :step-statement
           :reset-statement
           :clear-statement-bindings
           :statement-column-value
           :statement-column-names
           :statement-bind-parameter-names
           :bind-parameter
           :execute-non-query
           :execute-to-list
           :execute-single
           :execute-single/named
           :execute-one-row-m-v/named
           :execute-to-list/named
           :execute-non-query/named
           :execute-one-row-m-v
           :last-insert-rowid
           :with-transaction
           :with-open-database))

(in-package :sqlite)

(define-condition sqlite-error (simple-error)
  ((handle     :initform nil :initarg :db-handle
               :reader sqlite-error-db-handle)
   (error-code :initform nil :initarg :error-code
               :reader sqlite-error-code)
   (error-msg  :initform nil :initarg :error-msg
               :reader sqlite-error-message)
   (statement  :initform nil :initarg :statement
               :reader sqlite-error-statement)
   (sql        :initform nil :initarg :sql
               :reader sqlite-error-sql)))

(define-condition sqlite-constraint-error (sqlite-error)
  ())

(defun sqlite-error (error-code message &key
                     statement
                     (db-handle (if statement (db statement)))
                     (sql-text (if statement (sql statement))))
  (error (if (eq error-code :constraint)
             'sqlite-constraint-error
             'sqlite-error)
         :format-control (if (listp message) (first message) message)
         :format-arguments (if (listp message) (rest message))
         :db-handle db-handle
         :error-code error-code
         :error-msg (if (and db-handle error-code)
                        (sqlite-ffi:sqlite3-errmsg (handle db-handle)))
         :statement statement
         :sql sql-text))

(defmethod print-object :after ((obj sqlite-error) stream)
  (unless *print-escape*
    (when (or (and (sqlite-error-code obj)
                   (not (eq (sqlite-error-code obj) :ok)))
              (sqlite-error-message obj))
      (format stream "~&Code ~A: ~A."
              (or (sqlite-error-code obj) :OK)
              (or (sqlite-error-message obj) "no message")))
    (when (sqlite-error-db-handle obj)
      (format stream "~&Database: ~A"
              (database-path (sqlite-error-db-handle obj))))
    (when (sqlite-error-sql obj)
      (format stream "~&SQL: ~A" (sqlite-error-sql obj)))))

;(declaim (optimize (speed 3) (safety 0) (debug 0)))

(defclass sqlite-handle ()
  ((handle :accessor handle)
   (database-path :accessor database-path)
   (cache :accessor cache)
   (statements :initform nil :accessor sqlite-handle-statements))
  (:documentation "Class that encapsulates the connection to the database. Use connect and disconnect."))

(defmethod initialize-instance :after ((object sqlite-handle) &key (database-path ":memory:") &allow-other-keys)
  (cffi:with-foreign-object (ppdb 'sqlite-ffi:p-sqlite3)
    (let ((error-code (sqlite-ffi:sqlite3-open database-path ppdb)))
      (if (eq error-code :ok)
          (setf (handle object) (cffi:mem-ref ppdb 'sqlite-ffi:p-sqlite3)
                (database-path object) database-path)
          (sqlite-error error-code (list "Could not open sqlite3 database ~A" database-path)))))
  (setf (cache object) (make-instance 'sqlite.cache:mru-cache :cache-size 16 :destructor #'really-finalize-statement)))

(defun connect (database-path &key busy-timeout)
  "Connect to the sqlite database at the given DATABASE-PATH. Returns the SQLITE-HANDLE connected to the database. Use DISCONNECT to disconnect.
   Operations will wait for locked databases for up to BUSY-TIMEOUT milliseconds; if BUSY-TIMEOUT is NIL, then operations on locked databases will fail immediately."
  (let ((db (make-instance 'sqlite-handle
                           :database-path (etypecase database-path
                                            (string database-path)
                                            (pathname (namestring database-path))))))
    (when busy-timeout
      (set-busy-timeout db busy-timeout))
    db))

(defun set-busy-timeout (db milliseconds)
  "Sets the maximum amount of time to wait for a locked database."
  (sqlite-ffi:sqlite3-busy-timeout (handle db) milliseconds))

(defun disconnect (handle)
  "Disconnects the given HANDLE from the database. All further operations on the handle are invalid."
  (sqlite.cache:purge-cache (cache handle))
  (iter (with statements = (copy-list (sqlite-handle-statements handle)))
        (declare (dynamic-extent statements))
        (for statement in statements)
        (really-finalize-statement statement))
  (let ((error-code (sqlite-ffi:sqlite3-close (handle handle))))
    (unless (eq error-code :ok)
      (sqlite-error error-code "Could not close sqlite3 database." :db-handle handle))
    (slot-makunbound handle 'handle)))

(defclass sqlite-statement ()
  ((db :reader db :initarg :db)
   (handle :accessor handle)
   (sql :reader sql :initarg :sql)
   (columns-count :accessor resultset-columns-count)
   (columns-names :accessor resultset-columns-names :reader statement-column-names)
   (parameters-count :accessor parameters-count)
   (parameters-names :accessor parameters-names :reader statement-bind-parameter-names))
  (:documentation "Class that represents the prepared statement."))

(defmethod initialize-instance :after ((object sqlite-statement) &key &allow-other-keys)
  (cffi:with-foreign-object (p-statement 'sqlite-ffi:p-sqlite3-stmt)
    (cffi:with-foreign-object (p-tail '(:pointer :char))
      (cffi:with-foreign-string (sql (sql object))
        (let ((error-code (sqlite-ffi:sqlite3-prepare (handle (db object)) sql -1 p-statement p-tail)))
          (unless (eq error-code :ok)
            (sqlite-error error-code "Could not prepare an sqlite statement."
                          :db-handle (db object) :sql-text (sql object)))
          (unless (zerop (cffi:mem-ref (cffi:mem-ref p-tail '(:pointer :char)) :uchar))
            (sqlite-error nil "SQL string contains more than one SQL statement." :sql-text (sql object)))
          (setf (handle object) (cffi:mem-ref p-statement 'sqlite-ffi:p-sqlite3-stmt)
                (resultset-columns-count object) (sqlite-ffi:sqlite3-column-count (handle object))
                (resultset-columns-names object) (loop
                                                    for i below (resultset-columns-count object)
                                                    collect (sqlite-ffi:sqlite3-column-name (handle object) i))
                (parameters-count object) (sqlite-ffi:sqlite3-bind-parameter-count (handle object))
                (parameters-names object) (loop
                                             for i from 1 to (parameters-count object)
                                             collect (sqlite-ffi:sqlite3-bind-parameter-name (handle object) i))))))))

(defun prepare-statement (db sql)
  "Prepare the statement to the DB that will execute the commands that are in SQL.

Returns the SQLITE-STATEMENT.

SQL must contain exactly one statement.
SQL may have some positional (not named) parameters specified with question marks.

Example:

 select name from users where id = ?"
  (or (let ((statement (sqlite.cache:get-from-cache (cache db) sql)))
        (when statement
          (clear-statement-bindings statement))
        statement)
      (let ((statement (make-instance 'sqlite-statement :db db :sql sql)))
        (push statement (sqlite-handle-statements db))
        statement)))

(defun really-finalize-statement (statement)
  (setf (sqlite-handle-statements (db statement))
        (delete statement (sqlite-handle-statements (db statement))))
  (sqlite-ffi:sqlite3-finalize (handle statement))
  (slot-makunbound statement 'handle))

(defun finalize-statement (statement)
  "Finalizes the statement and signals that associated resources may be released.
Note: does not immediately release resources because statements are cached."
  (reset-statement statement)
  (sqlite.cache:put-to-cache (cache (db statement)) (sql statement) statement))

(defun step-statement (statement)
  "Steps to the next row of the resultset of STATEMENT.
Returns T is successfully advanced to the next row and NIL if there are no more rows."
  (let ((error-code (sqlite-ffi:sqlite3-step (handle statement))))
    (case error-code
      (:done nil)
      (:row t)
      (t
       (sqlite-error error-code "Error while stepping an sqlite statement." :statement statement)))))

(defun reset-statement (statement)
  "Resets the STATEMENT and prepare it to be called again."
  (let ((error-code (sqlite-ffi:sqlite3-reset (handle statement))))
    (unless (eq error-code :ok)
      (sqlite-error error-code "Error while resetting an sqlite statement." :statement statement))))

(defun clear-statement-bindings (statement)
  "Sets all binding values to NULL."
  (let ((error-code (sqlite-ffi:sqlite3-clear-bindings (handle statement))))
    (unless (eq error-code :ok)
      (sqlite-error error-code "Error while clearing bindings of an sqlite statement."
                    :statement statement))))

(defun statement-column-value (statement column-number)
  "Returns the COLUMN-NUMBER-th column's value of the current row of the STATEMENT. Columns are numbered from zero.
Returns:
 * NIL for NULL
 * INTEGER for integers
 * DOUBLE-FLOAT for floats
 * STRING for text
 * (SIMPLE-ARRAY (UNSIGNED-BYTE 8)) for BLOBs"
  (let ((type (sqlite-ffi:sqlite3-column-type (handle statement) column-number)))
    (ecase type
      (:null nil)
      (:text (sqlite-ffi:sqlite3-column-text (handle statement) column-number))
      (:integer (sqlite-ffi:sqlite3-column-int64 (handle statement) column-number))
      (:float (sqlite-ffi:sqlite3-column-double (handle statement) column-number))
      (:blob (let* ((blob-length (sqlite-ffi:sqlite3-column-bytes (handle statement) column-number))
                    (result (make-array (the fixnum blob-length) :element-type '(unsigned-byte 8)))
                    (blob (sqlite-ffi:sqlite3-column-blob (handle statement) column-number)))
               (loop
                  for i below blob-length
                  do (setf (aref result i) (cffi:mem-aref blob :unsigned-char i)))
               result)))))

(defmacro with-prepared-statement (statement-var (db sql parameters-var) &body body)
  (let ((i-var (gensym "I"))
        (value-var (gensym "VALUE")))
    `(let ((,statement-var (prepare-statement ,db ,sql)))
       (unwind-protect
            (progn
              (iter (for ,i-var from 1)
                    (declare (type fixnum ,i-var))
                    (for ,value-var in ,parameters-var)
                    (bind-parameter ,statement-var ,i-var ,value-var))
              ,@body)
         (finalize-statement ,statement-var)))))

(defmacro with-prepared-statement/named (statement-var (db sql parameters-var) &body body)
  (let ((name-var (gensym "NAME"))
        (value-var (gensym "VALUE")))
    `(let ((,statement-var (prepare-statement ,db ,sql)))
       (unwind-protect
            (progn
              (iter (for (,name-var ,value-var) on ,parameters-var by #'cddr)
                    (bind-parameter ,statement-var (string ,name-var) ,value-var))
              ,@body)
         (finalize-statement ,statement-var)))))

(defun execute-non-query (db sql &rest parameters)
  "Executes the query SQL to the database DB with given PARAMETERS. Returns nothing.

Example:

\(execute-non-query db \"insert into users (user_name, real_name) values (?, ?)\" \"joe\" \"Joe the User\")

See BIND-PARAMETER for the list of supported parameter types."
  (declare (dynamic-extent parameters))
  (with-prepared-statement statement (db sql parameters)
    (step-statement statement)))

(defun execute-non-query/named (db sql &rest parameters)
  "Executes the query SQL to the database DB with given PARAMETERS. Returns nothing.

PARAMETERS is a list of alternating parameter names and values.

Example:

\(execute-non-query db \"insert into users (user_name, real_name) values (:name, :real_name)\" \":name\" \"joe\" \":real_name\" \"Joe the User\")

See BIND-PARAMETER for the list of supported parameter types."
  (declare (dynamic-extent parameters))
  (with-prepared-statement/named statement (db sql parameters)
    (step-statement statement)))

(defun execute-to-list (db sql &rest parameters)
  "Executes the query SQL to the database DB with given PARAMETERS. Returns the results as list of lists.

Example:

\(execute-to-list db \"select id, user_name, real_name from users where user_name = ?\" \"joe\")
=>
\((1 \"joe\" \"Joe the User\")
 (2 \"joe\" \"Another Joe\")) 

See BIND-PARAMETER for the list of supported parameter types."
  (declare (dynamic-extent parameters))
  (with-prepared-statement stmt (db sql parameters)
    (let (result)
      (loop (if (step-statement stmt)
                (push (iter (for i from 0 below (the fixnum (sqlite-ffi:sqlite3-column-count (handle stmt))))
                            (declare (type fixnum i))
                            (collect (statement-column-value stmt i)))
                      result)
                (return)))
      (nreverse result))))

(defun execute-to-list/named (db sql &rest parameters)
  "Executes the query SQL to the database DB with given PARAMETERS. Returns the results as list of lists.

PARAMETERS is a list of alternating parameters names and values.

Example:

\(execute-to-list db \"select id, user_name, real_name from users where user_name = :user_name\" \":user_name\" \"joe\")
=>
\((1 \"joe\" \"Joe the User\")
 (2 \"joe\" \"Another Joe\")) 

See BIND-PARAMETER for the list of supported parameter types."
  (declare (dynamic-extent parameters))
  (with-prepared-statement/named stmt (db sql parameters)
    (let (result)
      (loop (if (step-statement stmt)
                (push (iter (for i from 0 below (the fixnum (sqlite-ffi:sqlite3-column-count (handle stmt))))
                            (declare (type fixnum i))
                            (collect (statement-column-value stmt i)))
                      result)
                (return)))
      (nreverse result))))

(defun execute-one-row-m-v (db sql &rest parameters)
  "Executes the query SQL to the database DB with given PARAMETERS. Returns the first row as multiple values.

Example:
\(execute-one-row-m-v db \"select id, user_name, real_name from users where id = ?\" 1)
=>
\(values 1 \"joe\" \"Joe the User\")

See BIND-PARAMETER for the list of supported parameter types."
  (with-prepared-statement stmt (db sql parameters)
    (if (step-statement stmt)
        (return-from execute-one-row-m-v
          (values-list (iter (for i from 0 below (the fixnum (sqlite-ffi:sqlite3-column-count (handle stmt))))
                             (declare (type fixnum i))
                             (collect (statement-column-value stmt i)))))
        (return-from execute-one-row-m-v
          (values-list (loop repeat (the fixnum (sqlite-ffi:sqlite3-column-count (handle stmt))) collect nil))))))

(defun execute-one-row-m-v/named (db sql &rest parameters)
  "Executes the query SQL to the database DB with given PARAMETERS. Returns the first row as multiple values.

PARAMETERS is a list of alternating parameters names and values.

Example:
\(execute-one-row-m-v db \"select id, user_name, real_name from users where id = :id\" \":id\" 1)
=>
\(values 1 \"joe\" \"Joe the User\")

See BIND-PARAMETER for the list of supported parameter types."
  (with-prepared-statement/named stmt (db sql parameters)
    (if (step-statement stmt)
        (return-from execute-one-row-m-v/named
          (values-list (iter (for i from 0 below (the fixnum (sqlite-ffi:sqlite3-column-count (handle stmt))))
                             (declare (type fixnum i))
                             (collect (statement-column-value stmt i)))))
        (return-from execute-one-row-m-v/named
          (values-list (loop repeat (the fixnum (sqlite-ffi:sqlite3-column-count (handle stmt))) collect nil))))))

(defun statement-parameter-index (statement parameter-name)
  (sqlite-ffi:sqlite3-bind-parameter-index (handle statement) parameter-name))

(defun bind-parameter (statement parameter value)
  "Sets the PARAMETER-th parameter in STATEMENT to the VALUE.
PARAMETER may be parameter index (starting from 1) or parameters name.
Supported types:
 * NULL. Passed as NULL
 * INTEGER. Passed as an 64-bit integer
 * STRING. Passed as a string
 * FLOAT. Passed as a double
 * (VECTOR (UNSIGNED-BYTE 8)) and VECTOR that contains integers in range [0,256). Passed as a BLOB"
  (let ((index (etypecase parameter
                 (integer parameter)
                 (string (statement-parameter-index statement parameter)))))
    (declare (type fixnum index))
    (let ((error-code (typecase value
                        (null (sqlite-ffi:sqlite3-bind-null (handle statement) index))
                        (integer (sqlite-ffi:sqlite3-bind-int64 (handle statement) index value))
                        (double-float (sqlite-ffi:sqlite3-bind-double (handle statement) index value))
                        (real (sqlite-ffi:sqlite3-bind-double (handle statement) index (coerce value 'double-float)))
                        (string (sqlite-ffi:sqlite3-bind-text (handle statement) index value -1 (sqlite-ffi:destructor-transient)))
                        ((vector (unsigned-byte 8)) (cffi:with-pointer-to-vector-data (ptr value)
                                                      (sqlite-ffi:sqlite3-bind-blob (handle statement) index ptr (length value) (sqlite-ffi:destructor-transient))))
                        (vector (cffi:with-foreign-object (array :unsigned-char (length value))
                                  (loop
                                     for i from 0 below (length value)
                                     do (setf (cffi:mem-aref array :unsigned-char i) (aref value i)))
                                  (sqlite-ffi:sqlite3-bind-blob (handle statement) index array (length value) (sqlite-ffi:destructor-transient))))
                        (t
                         (sqlite-error nil
                                       (list "Do not know how to pass value ~A of type ~A to sqlite."
                                             value (type-of value))
                                       :statement statement)))))
      (unless (eq error-code :ok)
        (sqlite-error error-code
                      (list "Error when binding parameter ~A to value ~A." parameter value)
                      :statement statement)))))

(defun execute-single (db sql &rest parameters)
  "Executes the query SQL to the database DB with given PARAMETERS. Returns the first column of the first row as single value.

Example:
\(execute-single db \"select user_name from users where id = ?\" 1)
=>
\"joe\"

See BIND-PARAMETER for the list of supported parameter types."
  (declare (dynamic-extent parameters))
  (with-prepared-statement stmt (db sql parameters)
    (if (step-statement stmt)
        (statement-column-value stmt 0)
        nil)))

(defun execute-single/named (db sql &rest parameters)
  "Executes the query SQL to the database DB with given PARAMETERS. Returns the first column of the first row as single value.

PARAMETERS is a list of alternating parameters names and values.

Example:
\(execute-single db \"select user_name from users where id = :id\" \":id\" 1)
=>
\"joe\"

See BIND-PARAMETER for the list of supported parameter types."
  (declare (dynamic-extent parameters))
  (with-prepared-statement/named stmt (db sql parameters)
    (if (step-statement stmt)
        (statement-column-value stmt 0)
        nil)))

(defun last-insert-rowid (db)
  "Returns the auto-generated ID of the last inserted row on the database connection DB."
  (sqlite-ffi:sqlite3-last-insert-rowid (handle db)))

(defmacro with-transaction (db &body body)
  "Wraps the BODY inside the transaction."
  (let ((ok (gensym "TRANSACTION-COMMIT-"))
        (db-var (gensym "DB-")))
    `(let (,ok
           (,db-var ,db))
       (execute-non-query ,db-var "begin transaction")
       (unwind-protect
            (multiple-value-prog1
                (progn ,@body)
              (setf ,ok t))
         (if ,ok
             (execute-non-query ,db-var "commit transaction")
             (execute-non-query ,db-var "rollback transaction"))))))

(defmacro with-open-database ((db path &key busy-timeout) &body body)
  `(let ((,db (connect ,path :busy-timeout ,busy-timeout)))
     (unwind-protect
          (progn ,@body)
       (disconnect ,db))))

(defmacro-driver (FOR vars IN-SQLITE-QUERY query-expression ON-DATABASE db &optional WITH-PARAMETERS parameters)
  (let ((statement (gensym "STATEMENT-"))
        (kwd (if generate 'generate 'for)))
    `(progn (with ,statement = (prepare-statement ,db ,query-expression))
            (finally-protected (when ,statement (finalize-statement ,statement)))
            ,@(when parameters
                    (list `(initially ,@(iter (for i from 1)
                                              (for value in parameters)
                                              (collect `(sqlite:bind-parameter ,statement ,i ,value))))))
            (,kwd ,(if (symbolp vars)
                       `(values ,vars)
                       `(values ,@vars))
                  next (progn (if (step-statement ,statement)
                                  (values ,@(iter (for i from 0 below (if (symbolp vars) 1 (length vars)))
                                                  (collect `(statement-column-value ,statement ,i))))
                                  (terminate)))))))

(defmacro-driver (FOR vars IN-SQLITE-QUERY/NAMED query-expression ON-DATABASE db &optional WITH-PARAMETERS parameters)
  (let ((statement (gensym "STATEMENT-"))
        (kwd (if generate 'generate 'for)))
    `(progn (with ,statement = (prepare-statement ,db ,query-expression))
            (finally-protected (when ,statement (finalize-statement ,statement)))
            ,@(when parameters
                    (list `(initially ,@(iter (for (name value) on parameters by #'cddr)
                                              (collect `(sqlite:bind-parameter ,statement ,name ,value))))))
            (,kwd ,(if (symbolp vars)
                       `(values ,vars)
                       `(values ,@vars))
                  next (progn (if (step-statement ,statement)
                                  (values ,@(iter (for i from 0 below (if (symbolp vars) 1 (length vars)))
                                                  (collect `(statement-column-value ,statement ,i))))
                                  (terminate)))))))


(defmacro-driver (FOR vars ON-SQLITE-STATEMENT statement)
  (let ((statement-var (gensym "STATEMENT-"))
        (kwd (if generate 'generate 'for)))
    `(progn (with ,statement-var = ,statement)
            (,kwd ,(if (symbolp vars)
                       `(values ,vars)
                       `(values ,@vars))
                  next (progn (if (step-statement ,statement-var)
                                  (values ,@(iter (for i from 0 below (if (symbolp vars) 1 (length vars)))
                                                  (collect `(statement-column-value ,statement-var ,i))))
                                  (terminate)))))))