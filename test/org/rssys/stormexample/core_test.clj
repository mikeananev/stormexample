(ns org.rssys.stormexample.core-test
  (:require [clojure.test :refer [deftest testing is]]
            [matcho.core :refer [match]]
            [org.rssys.stormexample.core :as sut]))


(deftest ^:unit a-test
  (testing "simple test."
    (is (= 1 1))
    (match {:a 1} {:a int?})))
