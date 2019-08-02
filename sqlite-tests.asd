(defsystem :sqlite-tests
  :name "sqlite-tests"
  :author "Kalyanov Dmitry <Kalyanov.Dmitry@gmail.com>"
  :maintainer "Jacek Złydach <cl-sqlite@jacek.zlydach.pl>"
  :description "Tests for CL-SQLITE, an interface to the SQLite embedded relational database engine."
  :homepage "https://common-lisp.net/project/cl-sqlite/"
  :source-control (:git "git@github.com:TeMPOraL/cl-sqlite.git")
  :bug-tracker "https://github.com/TeMPOraL/cl-sqlite/issues"
  :version "0.2.1"
  :license "Public Domain"

  :components ((:file "sqlite-tests"))

  :depends-on (:fiveam :sqlite :bordeaux-threads))
