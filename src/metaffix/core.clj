(ns metaffix.core
  (:use [clojure.tools.cli :only [cli]])
  (:import [net.spy.memcached CachedData CASMutator CASMutation]
           [net.spy.memcached.transcoders Transcoder])
  (:require [metaffix.view :as view]
            [metaffix.tika :as tika]
            [clojure.string :as string]
            [clojure.walk :as walk]
            [cheshire.core :as json]
            [clj-http.client :as http]
            [clojurewerkz.spyglass.couchbase :as cbcli]
            [clojurewerkz.spyglass.client :as cb])
  (:gen-class))

(def json-transcoder
  (proxy [Transcoder] []
    (asyncDecode [_] false)
    (decode [bs] (json/parse-string (String. (.getData bs)) true))
    (encode [o] (CachedData. 0 (.getBytes (json/encode o)) CachedData/MAX_SIZE))
    (getMaxSize [] CachedData/MAX_SIZE)))

(defn cbitem
  "Fetch JSON item with `key` from `conn`"
  [conn k]
  (cb/get conn k json-transcoder))

(defn cb-update
  "Atomically update a JSON item with the result of applying f to it."
  [conn k f]
   (let [mutation
         (proxy [CASMutation] []
           (getNewValue [current]
             (f current)))
         mutator (CASMutator. conn json-transcoder)]
     (.cas mutator k nil 0 mutation)))

(defn capi-bases
  "Get CAPI base URL for a Couchbase cluster"
  [couchbasebase]
  (->> (http/get (str couchbasebase "pools/default") {:as :json})
       :body
       :nodes
       (mapv :couchApiBase)))

(defn cbfs-files [capis bucket]
  (->> (view/view-seq
         (str (rand-nth capis) bucket "/_design/cbfs/_view/file_browse")
         nil {} ; All the files
         {:include_docs true})
       (map (comp (juxt (comp :id :meta) :json) :doc))))

(defn open-blob
  [cbconn oid candidate-nodes]
  (first (drop-while nil? (map (fn [nodename]
                                 (let [nodedata (cbitem cbconn (str "/" (name nodename)))
                                       bloburl (str "http://" (:addr nodedata) (:bindaddr nodedata) "/.cbfs/blob/" oid)]
                                   (try (:body (http/get bloburl {:as :stream})) (catch Exception e nil))))
                               candidate-nodes))))
(defn process-blob
  "Fetch a blob by OID. Will try each node marked as having the blob until one succeeds,
   else returns nil. On success returns the value of calling f on the resulting stream."
  [cbconn oid f]
  (let [blobdata (cbitem cbconn (str "/" oid))
        candidate-nodes (keys (:nodes blobdata))
        blobstream (open-blob cbconn oid candidate-nodes)]
    (when blobstream
      (with-open [bs blobstream]
        (f bs)))))

(defn metaffix [{cburl :couchbase cbbucket :bucket cbpass :password
                 maxsize :max-size minsize :min-size}]
  (let [cbconn (cbcli/connection [(str cburl "pools")] cbbucket cbpass)
        capis (capi-bases cburl)
        eligible-files (filter (fn [[file-id {:keys [length userdata oid]}]]
                                 (and
                                  ; Not bigger than maxsize
                                  (>= maxsize length)
                                  ; Not smaller than minsize
                                  (<= minsize length)
                                  ; Unprocessed or out of date
                                  (or (nil? (:metaffix userdata))
                                      (not= (get-in userdata [:metaffix :oid]) oid)))) (cbfs-files capis cbbucket))]
    (doseq [[id {:keys [oid]}] eligible-files]
      (let [metadata (process-blob cbconn oid tika/parse)]
        (if metadata
          (if (try (cb-update cbconn id #(update-in % [:userdata]
                                               assoc :metaffix (assoc metadata :oid oid)))
                (catch Exception e
                  (println "Exception updating metadata on file" e)))
            (println "Metadata affixed to file" id)
            (println "Error affixing metadata to file" id))
          (println "Error processing file" id)))))
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
                    ["-m" "--max-size" "Don't attempt to tag files larger than this size" :default 10485760 :parse-fn dehumanize]
                    ["-n" "--min-size" "Don't attempt to tag files smaller than this size" :default 0 :parse-fn dehumanize]
                    ["-h" "--help" "Show help" :default false :flag true])]
    (when (:help config)
      (println usage)
      (println "Sizes can take numbers suffixed with K, M, G.")
      (System/exit 0))
    (println "Config: " (pr-str config))
    (metaffix config)))
