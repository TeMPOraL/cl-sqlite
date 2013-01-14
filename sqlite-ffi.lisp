(defpackage :sqlite-ffi
  (:use :cl :cffi)
  (:export :error-code
           :p-sqlite3
           :sqlite3-open
           :sqlite3-close
           :sqlite3-errmsg
           :sqlite3-busy-timeout
           :p-sqlite3-stmt
           :sqlite3-prepare
           :sqlite3-finalize
           :sqlite3-step
           :sqlite3-reset
           :sqlite3-clear-bindings
           :sqlite3-column-count
           :sqlite3-column-type
           :sqlite3-column-text
           :sqlite3-column-int64
           :sqlite3-column-double
           :sqlite3-column-bytes
           :sqlite3-column-blob
           :sqlite3-column-name
           :sqlite3-bind-parameter-count
           :sqlite3-bind-parameter-name
           :sqlite3-bind-parameter-index
           :sqlite3-bind-double
           :sqlite3-bind-int64
           :sqlite3-bind-null
           :sqlite3-bind-text
           :sqlite3-bind-blob
           :destructor-transient
           :destructor-static
           :sqlite3-last-insert-rowid))

(in-package :sqlite-ffi)

(define-foreign-library sqlite3-lib
  (:darwin (:default "libsqlite3"))
  (:unix (:or "libsqlite3.so.0" "libsqlite3.so"))
  (t (:or (:default "libsqlite3") (:default "sqlite3"))))

(use-foreign-library sqlite3-lib)

(defcenum error-code
  (:OK 0)
  (:ERROR 1)
  (:INTERNAL 2)
  (:PERM 3)
  (:ABORT 4)
  (:BUSY 5)
  (:LOCKED 6)
  (:NOMEM 7)
  (:READONLY 8)
  (:INTERRUPT 9)
  (:IOERR 10)
  (:CORRUPT 11)
  (:NOTFOUND 12)
  (:FULL 13)
  (:CANTOPEN 14)
  (:PROTOCOL 15)
  (:EMPTY 16)
  (:SCHEMA 17)
  (:TOOBIG 18)
  (:CONSTRAINT 19)
  (:MISMATCH 20)
  (:MISUSE 21)
  (:NOLFS 22)
  (:AUTH 23)
  (:FORMAT 24)
  (:RANGE 25)
  (:NOTADB 26)
  (:ROW 100)
  (:DONE 101))

(defcstruct sqlite3)

(defctype p-sqlite3 (:pointer sqlite3))

(defcfun sqlite3-open error-code
  (filename :string)
  (db (:pointer p-sqlite3)))

(defcfun sqlite3-close error-code
  (db p-sqlite3))

(defcfun sqlite3-errmsg :string
  (db p-sqlite3))

(defcfun sqlite3-busy-timeout :int
  (db p-sqlite3)
  (ms :int))

(defcstruct sqlite3-stmt)

(defctype p-sqlite3-stmt (:pointer sqlite3-stmt))

(defcfun (sqlite3-prepare "sqlite3_prepare_v2") error-code
  (db p-sqlite3)
  (sql :string)
  (sql-length-bytes :int)
  (stmt (:pointer p-sqlite3-stmt))
  (tail (:pointer (:pointer :char))))

(defcfun sqlite3-finalize error-code
  (statement p-sqlite3-stmt))

(defcfun sqlite3-step error-code
  (statement p-sqlite3-stmt))

(defcfun sqlite3-reset error-code
  (statement p-sqlite3-stmt))

(defcfun sqlite3-clear-bindings error-code
  (statement p-sqlite3-stmt))

(defcfun sqlite3-column-count :int
  (statement p-sqlite3-stmt))

(defcenum type-code
  (:integer 1)
  (:float 2)
  (:text 3)
  (:blob 4)
  (:null 5))

(defcfun sqlite3-column-type type-code
  (statement p-sqlite3-stmt)
  (column-number :int))

(defcfun sqlite3-column-text :string
  (statement p-sqlite3-stmt)
  (column-number :int))

(defcfun sqlite3-column-int64 :int64
  (statement p-sqlite3-stmt)
  (column-number :int))

(defcfun sqlite3-column-double :double
  (statement p-sqlite3-stmt)
  (column-number :int))

(defcfun sqlite3-column-bytes :int
  (statement p-sqlite3-stmt)
  (column-number :int))

(defcfun sqlite3-column-blob :pointer
  (statement p-sqlite3-stmt)
  (column-number :int))

(defcfun sqlite3-column-name :string
  (statement p-sqlite3-stmt)
  (column-number :int))

(defcfun sqlite3-bind-parameter-count :int
  (statement p-sqlite3-stmt))

(defcfun sqlite3-bind-parameter-name :string
  (statement p-sqlite3-stmt)
  (column-number :int))

(defcfun sqlite3-bind-parameter-index :int
  (statement p-sqlite3-stmt)
  (name :string))

(defcfun sqlite3-bind-double error-code
  (statement p-sqlite3-stmt)
  (parameter-index :int)
  (value :double))

(defcfun sqlite3-bind-int64 error-code
  (statement p-sqlite3-stmt)
  (parameter-index :int)
  (value :int64))

(defcfun sqlite3-bind-null error-code
  (statement p-sqlite3-stmt)
  (parameter-index :int))

(defcfun sqlite3-bind-text error-code
  (statement p-sqlite3-stmt)
  (parameter-index :int)
  (value :string)
  (octets-count :int)
  (destructor :pointer))

(defcfun sqlite3-bind-blob error-code
  (statement p-sqlite3-stmt)
  (parameter-index :int)
  (value :pointer)
  (bytes-count :int)
  (destructor :pointer))

(defconstant destructor-transient-address (mod -1 (expt 2 (* 8 (cffi:foreign-type-size :pointer)))))

(defun destructor-transient () (cffi:make-pointer destructor-transient-address))

(defun destructor-static () (cffi:make-pointer 0))

(defcfun sqlite3-last-insert-rowid :int64
  (db p-sqlite3))
