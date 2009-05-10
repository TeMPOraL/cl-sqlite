(defsystem :sqlite
  :name "sqlite"
  :author "Kalyanov Dmitry <Kalyanov.Dmitry@gmail.com>"
  :version "0.1.4"
  :license "Public Domain"
  :components ((:file "sqlite-ffi")
               (:file "cache")
               (:file "sqlite" :depends-on ("sqlite-ffi" "cache")))
  :depends-on (:iterate :cffi))