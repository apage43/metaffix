(ns metaffix.view
  (:require [clj-http.client :as http]
            [cheshire.core :as json]))

(def ^:dynamic *view-chunk-size* 150)

(defn view-cursor [url & [parms]]
  {:url url
   :last-key nil
   :last-id nil
   :parms (merge {:reduce false} parms)
   :limit *view-chunk-size*
   :end false})

(defn fetch-view-chunk [curs]
  (let [{:keys [url last-key limit last-id skip parms]} curs
        rqparms (merge parms
                       {:limit limit}
                       (if skip {:skip skip})
                       (if last-key {:startkey (json/encode last-key)
                                     :startkey_docid last-id}))
        rq (http/get url {:query-params rqparms :as :json})
        jbody (:body rq)
        rows (:rows jbody)
        lastrow (last rows)
        newcurs (merge curs {:last-key (:key lastrow)
                             :last-id (:id lastrow)
                             :skip 1}
                       (if (empty? rows) {:end true}))]
    [newcurs rows]))

(defn view-iterator [cursatom]
  (lazy-seq
   (if (:end @cursatom) []
       (let [[nextcurs rows] (fetch-view-chunk @cursatom)]
         (reset! cursatom nextcurs)
         (concat rows (lazy-seq (view-iterator cursatom)))))))

(defn view-seq [url {:keys [start end params]}]
  (let [separms (merge {}
                       (if start {:startkey (json/encode start)})
                       (if end {:endkey (json/encode end)}))
        cursatom (atom (view-cursor url (merge separms params)))]
    (view-iterator cursatom)))
