(defsystem :sqlite
  :name "sqlite"
  :author "Kalyanov Dmitry <Kalyanov.Dmitry@gmail.com>"
  :maintainer "Jacek Złydach <cl-sqlite@jacek.zlydach.pl>"
  :description "CL-SQLITE package is an interface to the SQLite embedded relational database engine."
  :homepage "https://common-lisp.net/project/cl-sqlite/"
  :source-control (:git "git@github.com:TeMPOraL/cl-sqlite.git")
  :bug-tracker "https://github.com/TeMPOraL/cl-sqlite/issues"
  :version "0.2.1"
  :license "Public Domain"

  :components ((:file "sqlite-ffi")
               (:file "cache")
               (:file "sqlite" :depends-on ("sqlite-ffi" "cache")))

  :depends-on (:iterate :cffi)

  :in-order-to ((test-op (load-op sqlite-tests))))

(defmethod perform ((o asdf:test-op) (c (eql (find-system :sqlite))))
  (funcall (intern "RUN-ALL-SQLITE-TESTS" :sqlite-tests)))
