(ns pgloader.stats
  (:require [clojure.string :as str]))

(set! *warn-on-reflection* true)

(defrecord TableEntry
           [label
            read
            rows
            errs
            bytes
            rs-nanos
            ws-nanos
            total-nanos
            reject-data
            reject-log])

(defrecord Phase
           [entries])

(defn make-phase
  []
  (->Phase (atom [])))

(def state
  {:pre  (make-phase)
   :data (make-phase)
   :post (make-phase)})

(def ^:private fatal-error (atom false))

(defn fatal-error!
  "Signal that a fatal (non-row-level) load error occurred."
  []
  (reset! fatal-error true))

(defn fatal-error?
  "Return true if a fatal load error was signalled."
  []
  @fatal-error)

(defn clear!
  []
  (reset! fatal-error false)
  (doseq [p (vals state)]
    (reset! (:entries p) [])))

(defn new-entry!
  [phase label]
  (let [entry (map->TableEntry
               {:label label
                :read 0
                :rows 0
                :errs 0
                :bytes 0
                :rs-nanos 0
                :ws-nanos 0
                :total-nanos 0
                :reject-data nil
                :reject-log nil})]
    (swap! (:entries (phase state)) conj entry)
    entry))

(defn update-entry!
  [phase label & {:keys [read rows errs bytes rs-nanos ws-nanos total-nanos
                         reject-data reject-log]}]
  (let [entries-atom (:entries (phase state))]
    (swap! entries-atom
           (fn [entries]
             (mapv (fn [e]
                     (if (= (:label e) label)
                       (map->TableEntry
                        {:label label
                         :read (or read (:read e))
                         :rows (or rows (:rows e))
                         :errs (or errs (:errs e))
                         :bytes (or bytes (:bytes e))
                         :rs-nanos (or rs-nanos (:rs-nanos e))
                         :ws-nanos (or ws-nanos (:ws-nanos e))
                         :total-nanos (or total-nanos (:total-nanos e))
                         :reject-data (or reject-data (:reject-data e))
                         :reject-log (or reject-log (:reject-log e))})
                       e))
                   entries)))))

(defn entries
  [phase]
  @(:entries (phase state)))

(defn get-totals
  "Return aggregate {:rows :errs :bytes :total-nanos} for a phase."
  [phase]
  (reduce
   (fn [acc e]
     (-> acc
         (update :rows + (:rows e))
         (update :errs + (:errs e))
         (update :bytes + (:bytes e))
         (update :total-nanos + (:total-nanos e))))
   {:rows 0 :errs 0 :bytes 0 :total-nanos 0}
   (entries phase)))

(defn grand-totals
  "Return aggregate across all three phases."
  []
  (let [all (concat (entries :pre) (entries :data) (entries :post))]
    (reduce
     (fn [acc e]
       (-> acc
           (update :rows + (:rows e))
           (update :errs + (:errs e))
           (update :bytes + (:bytes e))
           (update :total-nanos + (:total-nanos e))))
     {:rows 0 :errs 0 :bytes 0 :total-nanos 0}
     all)))
