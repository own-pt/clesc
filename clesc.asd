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

(asdf:defsystem #:clesc
  :version "0.1"
  :description "Common Lisp interface to Elastic Search"
  :author "Fabricio Chalub <fchalub@br.ibm.com>"
  :license "Apache 2.0"
  :depends-on (#:drakma
               #:yason
               #:alexandria)
  :serial t
  :components ((:file "package")
               (:file "clesc")))

