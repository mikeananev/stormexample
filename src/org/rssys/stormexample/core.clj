(ns org.rssys.stormexample.core
  (:gen-class)
  (:require [io.pedestal.log :as log]
            [org.apache.storm.clojure :as storm]
            [org.apache.storm.config :as storm-config]
            )
  (:import (org.apache.storm.generated StormTopology)
           (org.apache.storm StormSubmitter)))


(defn set-global-exception-hook
  "Catch any uncaught exceptions and print them."
  []
  (Thread/setDefaultUncaughtExceptionHandler
    (reify Thread$UncaughtExceptionHandler
      (uncaughtException
        [_ thread ex]
        (println "uncaught exception" :thread (.getName thread) :desc ex)))))


(storm/defspout sentence-spout ["sentence"]
  [conf context collector]
  (let [sentences ["a little brown dog"
                   "the man petted the dog"
                   "four score and seven years ago"
                   "an apple a day keeps the doctor away"]]
    (storm/spout
      (nextTuple []
        (Thread/sleep 100)
        (storm/emit-spout! collector [(rand-nth sentences)])
        )
      (storm/ack [id]
        ;; You only need to define this method for reliable spouts
        ;; (such as one that reads off of a queue like Kestrel)
        ;; This is an unreliable spout, so it does nothing here
        ))))


(storm/defspout sentence-spout-parameterized ["word"] {:params [sentences] :prepare false}
  [collector]
  (Thread/sleep 500)
  (storm/emit-spout! collector [(rand-nth sentences)]))


(storm/defbolt split-sentence ["word"] [tuple collector]
  (let [words (.split (.getString tuple 0) " ")]
    (doseq [w words]
      (storm/emit-bolt! collector [w] :anchor tuple))
    (storm/ack! collector tuple)
    ))

(storm/defbolt word-count ["word" "count"] {:prepare true}
  [conf context collector]
  (let [counts (atom {})]
    (storm/bolt
      (execute [tuple]
        (let [word (.getString tuple 0)]
          (swap! counts (partial merge-with +) {word 1})
          (storm/emit-bolt! collector [word (@counts word)] :anchor tuple)
          (storm/ack! collector tuple)
          )))))


(defn mk-topology []

  (storm/topology
    {"1" (storm/spout-spec sentence-spout)
     "2" (storm/spout-spec (sentence-spout-parameterized
                             ["the cat jumped over the door"
                              "greetings from a faraway land"])
           :p 2)}
    {"3" (storm/bolt-spec {"1" :shuffle "2" :shuffle}
           split-sentence
           :p 5)
     "4" (storm/bolt-spec {"3" ["word"]}
           word-count
           :p 6)}))

(defn submit-topology! [name]
  (StormSubmitter/submitTopology
    name
    {storm-config/TOPOLOGY-DEBUG   true
     storm-config/TOPOLOGY-WORKERS 3}
    (mk-topology)
    ))


(comment
  (def local-cluster (LocalCluster.))
  (def local-cluster (storm/local-cluster))

  (.submitTopology
    ^LocalCluster local-cluster
    "test"
    {storm-config/TOPOLOGY-DEBUG   true
     storm-config/TOPOLOGY-WORKERS 3}
    ^StormTopology (mk-topology)
    )

  (.killTopology local-cluster "test")
  (.shutdown ^LocalCluster local-cluster)

  (def topology (submit-topology! "test"))
  )

(defn -main
  "entry point to app."
  [& args]
  (set-global-exception-hook)
  (log/info :msg "app is started." :args args)

  (submit-topology! "test")

  (Thread/sleep 1000)

  (System/exit 0))
