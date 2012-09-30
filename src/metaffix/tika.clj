(ns metaffix.tika
  (:import [org.apache.tika.parser Parser AutoDetectParser ParseContext]
           [org.apache.tika.metadata Metadata]
           [org.apache.tika Tika]
           [org.apache.tika.exception TikaException]
           [org.apache.tika.io TikaInputStream]
           [org.apache.tika.sax BodyContentHandler]
           [org.xml.sax.helpers DefaultHandler]))

(defn- cljify-metadata [metadata]
  (into {} (map (fn [n]
                  [(keyword n)
                  (let [vs (map (fn [v] (if (.matches (re-matcher #"\d+" v)) (read-string v) v))
                                (seq (.getValues metadata n)))]
                    (if (= 1 (count vs)) (first vs) vs))])
             (.names metadata))))

(defn parse [^java.io.InputStream istream]
  (let [parser (AutoDetectParser.)
        context (ParseContext.)
        metadata (Metadata.)
        handler (DefaultHandler.)]
    (doto context
      (.set Parser parser))
    (try
      (.parse parser (TikaInputStream/get istream) handler metadata context)
      (cljify-metadata metadata)
      (catch TikaException e
        {:parse-error (str e)}))))
