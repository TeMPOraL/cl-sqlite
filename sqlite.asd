(defsystem :sqlite
  :name "sqlite"
  :author "Kalyanov Dmitry <Kalyanov.Dmitry@gmail.com>"
  :version "0.1.6"
  :license "Public Domain"
  :components ((:file "sqlite-ffi")
               (:file "cache")
               (:file "sqlite" :depends-on ("sqlite-ffi" "cache")))
  :depends-on (:iterate :cffi)
  :in-order-to ((test-op (load-op sqlite-tests))))

(defmethod perform ((o asdf:test-op) (c (eql (find-system :sqlite))))
  (funcall #-allegro (intern "RUN-ALL-TESTS" :sqlite-tests)
           #+allegro (if (eq excl:*current-case-mode* :case-sensitive-lower)
                         (intern "run-all-tests" :sqlite-tests)
                       (intern "RUN-ALL-TESTS"))))
