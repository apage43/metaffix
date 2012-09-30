(ns metaffix.core
  (:use [clojure.tools.cli :only [cli]])
  (:require [metaffix.view :as view]
            [metaffix.tika :as tika]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [cheshire.core :as json]
            [clojurewerkz.spyglass.couchbase :as cbcli]
            [clj-http.client :as http]
            [clojurewerkz.spyglass.client :as cb])
  (:gen-class))

; Spyglass's gets is busted for non-existing keys
(defn gets [conn k]
  (let [retr (.gets conn k)]
    (if retr
      {:value (.getValue retr) :cas (.getCas retr)})))

(defn cbitem
  "Fetch JSON item with `key` from `conn`, and return parsed value with CAS id on metadata as :cas"
  [conn k]
  (let [item (gets conn k)]
    (if item (with-meta (json/parse-string (:value item) true) {:cas (:cas item)}))))

(defn cb-update
  "Atomically update a JSON item with the result of applying f to it."
  ([conn k f]
   (cb-update conn k f 1000))
  ([conn k f maxretries]
   (try
     (loop [retries maxretries]
      (if (pos? retries)
        (let [item (cbitem conn k)]
          (if (nil? item) :item_error
            (if (not= :ok
                      (cb/cas conn k (:cas (meta item)) (json/encode (f item))))
              (recur (dec retries))
              :ok))) :too_many_retries))
     (catch Exception e :item_error))))

(defn capi-bases
  "Get CAPI base URL for a Couchbase cluster"
  [couchbasebase]
  (->> (http/get (str couchbasebase "pools/default") {:as :json})
       :body
       :nodes
       (mapv :couchApiBase)))

(defn cbfs-files [capis bucket]
  (->> (view/view-seq (str (rand-nth capis) bucket "/_design/cbfs/_view/file_browse") nil {} {:include_docs true})
       (map (comp (juxt (comp :id :meta) :json) :doc))))

(defn fetch-blob
  "Fetch a blob by OID. Will try each node marked as having the blob until one succeeds, else returns nil. On success returns a stream."
  [cbconn oid]
  (let [blobdata (cbitem cbconn (str "/" oid))
        candidate-nodes (keys (:nodes blobdata))]
    (first (drop-while nil? (map (fn [nodename]
                                   (let [nodedata (cbitem cbconn (str "/" (name nodename)))
                                         bloburl (str "http://" (:addr nodedata) (:bindaddr nodedata) "/.cbfs/blob/" oid)]
                                     (try (:body (http/get bloburl {:as :stream})) (catch Exception e nil))))
                                 candidate-nodes)))))

(defn metaffix [{cburl :couchbase cbbucket :bucket cbpass :password maxsize :max-size}]
  (let [cbconn (cbcli/connection [(str cburl "pools")] cbbucket cbpass)
        capis (capi-bases cburl)
        eligible-files (filter (fn [[file-id {:keys [length userdata oid]}]]
                                 (and
                                  ; Not bigger than maxsize
                                  (<= maxsize length)
                                  ; Unprocessed or out of date
                                  (or (nil? (:metaffix userdata))
                                      (not= (get-in userdata [:metaffix :oid]) oid)))) (cbfs-files capis cbbucket))]
    (doseq [[id {:keys [oid]}] eligible-files]
      (let [item-stream (fetch-blob cbconn oid)]
        (if item-stream
          (let [metadata (assoc (tika/parse item-stream) :oid oid)]
            (if (= :ok (cb-update cbconn id
                                        #(update-in % [:userdata] assoc :metaffix metadata)))
              (println "Affixed metadata to file: " id)
              (println "WARN: Couldn't update file " id))
            (.close item-stream))
          (println "Could not fetch blob: " oid)))))
  (println "Done!"))

(def num-suffixes
  {"G" (* 1024 1024 1024)
   "M" (* 1024 1024)
   "K" 1024
   "" 1})

(defn dehumanize
  "Deprive a number of bytes of its human qualities."
  [numstr]
  (let [[_orig num unit] (re-matches #"([\d\.]+)(\w*)" numstr)
        scale (num-suffixes unit)] 
    (when (nil? scale) (throw (ex-info "I don't understand this number" {:input numstr})))
    (long (* scale (read-string num)))))

(defn -main
  [& args]
  (let [[config _trailers usage] (cli args
                    ["-c" "--couchbase" "URL of Couchbase cluster" :default "http://localhost:8091/"]
                    ["-b" "--bucket" "CBFS bucket name on Couchbase" :default "cbfs"]
                    ["-p" "--password" "Password for the Couchbase bucket" :default ""]
                    ["-m" "--max-size" "Don't attempt to tag files larger than this size. (Can use K, M, G suffix)" :default 10485760 :parse-fn dehumanize]
                    ["-h" "--help" "Show help" :default false :flag true])]
    (when (:help config)
      (println usage)
      (System/exit 0))
    (println "Config: " (pr-str config))
    (metaffix config)))
