;; Copyright 2015, 2016 IBM

;; Licensed under the Apache License, Version 2.0 (the "License");
;; you may not use this file except in compliance with the License.
;; You may obtain a copy of the License at

;;     http://www.apache.org/licenses/LICENSE-2.0

;; Unless required by applicable law or agreed to in writing, software
;; distributed under the License is distributed on an "AS IS" BASIS,
;; WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
;; See the License for the specific language governing permissions and
;; limitations under the License.

(in-package #:clesc)

(defparameter *es-endpoint* "http://localhost:9200")

(defparameter *debug-query-dsl* nil)

(defparameter *es-authentication* nil
  "A list each the first element is the user and the second is the
password." )

(defun encode-to-string (hash-table)
  (with-output-to-string (s)
    (yason:encode hash-table s)))

(defun query-aggregation (fields agg-nested)
  "Create an aggregation over FIELDS.  Each elements of the list is a
triple (FIELD ORDER-FIELD ORDER-DIRECTION), where FIELD is the field
name, ORDER-FIELD is the type of ordering (e.g., _term or _count) and
ORDER-DIRECTION is either asc or desc."
  (yason:with-object-element ("aggs")
    (yason:with-object ()
     (let ((agg (aux-agg fields))
	   (agg-nested  (aux-agg-nested agg-nested)))
       (cond ((and agg agg-nested) (append agg agg-nested))
	     (t agg))))))


(defun aux-agg (fields)
  (when fields
    (dolist (field-triple fields)
      (let ((field (if (listp field-triple) (first field-triple) field-triple))
	    (order-field (if (listp field-triple) (second field-triple) nil))
	    (order-dir (if (listp field-triple) (third field-triple) nil)))
	(yason:with-object-element (field)
	  (yason:with-object ()
	    (yason:with-object-element ("terms")
	      (yason:with-object ()
		(yason:encode-object-element "field" field)
		(when (and order-field order-dir)
		  (yason:with-object-element ("order")
		    (yason:with-object ()
		      (yason:encode-object-element order-field order-dir))))))))))))

(defun aux-agg-nested (agg-nested)
  (when agg-nested
    (funcall agg-nested)))

(defun wildcard? (text)
  "Check if text is a wildcard; that is, it is either empty or '*'."
  (or (emptyp text) (equal "*" text)))

(defun query-match (search-field texts)
  "When texts and search-field are strings: Create a MATCH fragment for TEXT;
 if text is wildcard, as defined by WILDCARD?, it is simply match_all otherwise,
 if search-field is not empty is a match against the specified field, else is a
 match against  all fields (_all).
  When texts and search-field are lists (they have to be the same length):
 Create a composition of MATCH queries, that each MATCH query is the nth texts element
 against the nth search-field element"
  (if (wildcard? texts)
      (yason:with-object ()
	(yason:with-object-element ("match_all")
	 (yason:with-object ()
	   (yason:encode-object-element "boost" 1.0))))
      (if (not (listp texts))
	  (yason:with-object ()
	    (yason:with-object-element ("match")
	     (yason:with-object ()
	       (yason:encode-object-element (if (not search-field) "_all" search-field) texts))))
	  (yason:with-array ()
	    (loop for text in texts for field in search-field do
		 (yason:with-object ()
		   (yason:with-object-element ("match")
		    (yason:with-object ()
		      (yason:encode-object-element field text)))))))))

(defun query-terms (terms)
  "Create a field query for TERMS."
  (yason:with-object-element ("filter")
    (yason:with-array ()
     (dolist (term terms)
       (yason:with-object ()
         (yason:with-object-element ("term")
           (yason:with-object ()
             (yason:encode-object-element (car term) (cadr term)))))))))

(define-condition query-error (error)
  ((message
    :initarg :message
    :accessor query-error-message
    :initform nil)))

(defun query-string (string)
  (yason:with-object ()
    (yason:with-object-element ("query_string")
      (yason:with-object ()
	(yason:encode-object-element "query" string))) ))

(defun query-search (&key text string terms agg-fields agg-nested fields-order from search-field (size 25) must-nested extra)
  "Creates the final query given a text to be matched, terms,
optionally aggregating certain fields."
  (yason:with-output-to-string* (:indent t)
    (yason:with-object ()
      (when (or agg-fields agg-nested)
        (query-aggregation agg-fields agg-nested))
      (when size
        (yason:encode-object-element "size" size))
      (when from
        (yason:encode-object-element "from" from))
      (yason:with-object-element ("query")
        (yason:with-object ()
          (yason:with-object-element ("bool")
            (yason:with-object ()
              (yason:with-object-element ("must")
                (yason:with-array ()
		  (append
		   (if (or text (not (or string must-nested))) (list (query-match search-field text)))
		   (if string (list (query-string string)))
		   (if must-nested (list (funcall must-nested))))))
              (when terms (query-terms terms))))))
      (when fields-order
        (dolist (field-info fields-order)
          (let ((name-field (if (listp field-info) (first field-info) nil))
                (order-field (if (listp field-info) (second field-info) nil)))
               (when (and name-field order-field)
                (yason:with-object-element ("sort")
                  (yason:with-object ()
                    (yason:with-object-element (name-field)
                      (yason:with-object ()
                        (yason:encode-object-element "order" order-field)))))))))
      (when extra
	(funcall extra)))))

(defun raw (field)
  "Returns the 'raw' name for a field. (that is, a field that is not
 analyzed)"
  (format nil "~a.raw" field))

(defun search/index/type (index type)
  "Returns the Elastic Search URL to access the search method for a
given index and type."
  (format nil "/~a/~a/_search" index type))

(defun search/index (index)
  "Returns the Elastic Search URL to access the search method for a
 given index."
  (format nil "/~a/_search" index))

(defun index-mappings (index)
  (gethash "mappings" (gethash index (call-es (format nil "/~a/_mappings" index)))))

(defun type-mappings (index type)
  (gethash "properties" (gethash type (index-mappings index))))

(defun %aggregate (index type field order-field order-direction)
  (call-es (search/index/type index type)
           :method :post
           :content (query-search :size 0 :agg-fields (list (list field order-field order-direction)))))

(defun es/refresh (index)
  (call-es (format nil "/~a/_refresh" index) :method :post))

(defun es/aggregate (index type field order-field order-direction)
  (gethash "buckets"
           (gethash field
                    (gethash "aggregations" (%aggregate index type field order-field order-direction)))))

(defun es/add (index type doc &key id)
  (let ((url (if id
                 (format nil "/~a/~a/~a" index type id)
                 (format nil "/~a/~a/" index type))))
    (call-es url :method :post :content (encode-to-string doc))))

(defun es/update (index type id doc)
  (let ((update (make-hash-table :test #'equal)))
    (setf (gethash "doc" update) doc)
    (call-es (format nil "/~a/~a/~a/_update" index type id) :method :post :content (encode-to-string update))))

(defun es/get (index type id)
  (call-es (format nil "/~a/~a/~a" index type id) :method :get))

(defun es/delete (index type id &key refresh)
  (call-es (format nil "/~a/~a/~a" index type id) :method :delete
	   :parameters (and refresh
			    `(("refresh" . ,refresh)))))

(defun es/search (index &key text search-field string terms facets fields-order from size agg-nested must-nested extra)
  (call-es (search/index index)
           :method :post
           :content (query-search :from from :size size :string string
                                  :text text :search-field search-field :terms terms :agg-fields facets :fields-order fields-order
				  :agg-nested agg-nested :must-nested must-nested :extra extra)))

(defun es/mapping (index json)
  "Creates an mapping. This function expects an index and the path to a JSON file with the mapping."
  (call-es index :method :put :content (alexandria:read-file-into-string json)))

(defun call-es (cmd &key (method :get) content parameters)
  (when *debug-query-dsl*
    (format *debug-io* "[~a ~a]~%" cmd content))
  (let ((stream (drakma:http-request
		 (format nil "~a~a" *es-endpoint* cmd)
		 :method method
                 :content content
		 :parameters parameters
		 :external-format-out :utf-8
		 :want-stream t
		 :content-type "application/json"
		 :basic-authorization *es-authentication*)))
    (setf (flexi-streams:flexi-stream-external-format stream) :utf-8)
    (let ((obj (yason:parse stream)))
      (close stream)
      obj)))


