(defsystem :sqlite-tests
  :name "sqlite-tests"
  :author "Kalyanov Dmitry <Kalyanov.Dmitry@gmail.com>"
  :version "0.1.4"
  :license "Public Domain"
  :components ((:file "sqlite-tests"))
  :depends-on (:fiveam :sqlite :bordeaux-threads))