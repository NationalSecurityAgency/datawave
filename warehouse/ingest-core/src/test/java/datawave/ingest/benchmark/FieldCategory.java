package datawave.ingest.benchmark;

/**
 * How a benchmark field is configured, which decides which key-generation paths it exercises.
 */
enum FieldCategory {
    /**
     * Stored in the shard event table only. Not indexed, so it produces one event key and nothing else.
     */
    EVENT_ONLY,

    /**
     * Forward indexed and stored. Produces an event key, a field index key and a global index key, and is the category that drives the per-term loops in
     * {@code createColumns}.
     */
    INDEXED,

    /**
     * Forward indexed but not stored in the event. Exercises the early return in {@code createShardEventColumn}, so it produces field index and global index
     * keys but no event key.
     */
    INDEX_ONLY,

    /**
     * Tokenized and index only. Runs the Lucene analyzer in {@code tokenizeField}, contributing one indexed term per distinct token plus a term-frequency key
     * per token/zone pair from {@code flushTokenOffsetCache}. The heaviest category by a wide margin.
     */
    TOKENIZED_INDEX_ONLY;

    /**
     * Category for a field index under the repeating assignment used by {@link BenchmarkEventGenerator}. Fields cycle event-only, indexed, index-only,
     * tokenized so every event carries all four.
     *
     * @param fieldIndex
     *            zero based field index
     * @return the category for that field
     */
    static FieldCategory forIndex(int fieldIndex) {
        switch (fieldIndex % 4) {
            case 0:
                return EVENT_ONLY;
            case 1:
                return INDEXED;
            case 2:
                return INDEX_ONLY;
            default:
                return TOKENIZED_INDEX_ONLY;
        }
    }
}
