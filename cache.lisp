(defpackage :sqlite.cache
  (:use :cl :iter)
  (:export :mru-cache
           :get-from-cache
           :put-to-cache
           :purge-cache))

(in-package :sqlite.cache)

;(declaim (optimize (speed 3) (safety 0) (debug 0)))

(defclass mru-cache ()
  ((objects-table :accessor objects-table :initform (make-hash-table :test 'equal))
   (last-access-time-table :accessor last-access-time-table :initform (make-hash-table :test 'equal))
   (total-cached :type fixnum :accessor total-cached :initform 0)
   (cache-size :type fixnum :accessor cache-size :initarg :cache-size :initform 100)
   (destructor :accessor destructor :initarg :destructor :initform #'identity)))

(defun get-from-cache (cache id)
  (let ((available-objects-stack (gethash id (objects-table cache))))
    (when (and available-objects-stack (> (length (the vector available-objects-stack)) 0))
      (decf (the fixnum (total-cached cache)))
      (setf (gethash id (last-access-time-table cache)) (get-internal-run-time))
      (vector-pop (the vector available-objects-stack)))))

(defun remove-empty-objects-stacks (cache)
  (let ((table (objects-table cache)))
   (maphash (lambda (key value)
              (declare (type vector value))
              (when (zerop (length value))
                (remhash key table)
                (remhash key (last-access-time-table cache))))
            table)))

(defun pop-from-cache (cache)
  (let ((id (iter (for (id time) in-hashtable (last-access-time-table cache))
                  (when (not (zerop (length (the vector (gethash id (objects-table cache))))))
                    (finding id minimizing (the fixnum time))))))
    (let ((object (vector-pop (gethash id (objects-table cache)))))
      (funcall (destructor cache) object)))
  (remove-empty-objects-stacks cache)
  (decf (the fixnum (total-cached cache))))

(defun put-to-cache (cache id object)
  (when (>= (the fixnum (total-cached cache)) (the fixnum (cache-size cache)))
    (pop-from-cache cache))
  (let ((available-objects-stack (or (gethash id (objects-table cache))
                                      (setf (gethash id (objects-table cache)) (make-array 0 :adjustable t :fill-pointer t)))))
    (vector-push-extend object available-objects-stack)
    (setf (gethash id (last-access-time-table cache)) (get-internal-run-time))
    (incf (the fixnum (total-cached cache)))
    object))

(defun purge-cache (cache)
  (iter (for (id items) in-hashtable (objects-table cache))
        (declare (ignorable id))
        (when items
          (iter (for item in-vector (the vector items))
                (funcall (destructor cache) item)))))