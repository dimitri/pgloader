;;;
;;; Tools to handle internal queueing, using lparallel.queue
;;;
(in-package :pgloader.queue)

(defun map-pop-queue (queue process-row-fn)
  "Consume the whole of QUEUE, calling PROCESS-ROW-FN on each row. The QUEUE
   must signal end of data by the element :end-of-data.

Returns how many rows where processed from the queue."
  (loop
     for row = (lq:pop-queue queue)
     until (eq row :end-of-data)
     counting row into count
     do (funcall process-row-fn row)
     finally (return count)))

(defun map-push-queue (queue map-row-fn &rest initial-args)
  "Apply MAP-ROW-FN on INITIAL-ARGS and a function of ROW that will push the
row into the queue. When MAP-ROW-FN returns, push :end-of-data in the queue.

Returns whatever MAP-ROW-FN did return."
  (prog1
      (apply map-row-fn (append initial-args
				(list
				 (lambda (row)
				   (lq:push-queue row queue)))))
    (lq:push-queue :end-of-data queue)))

