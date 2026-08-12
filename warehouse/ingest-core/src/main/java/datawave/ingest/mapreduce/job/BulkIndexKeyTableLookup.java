package datawave.ingest.mapreduce.job;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * A job-wide, immutable mapping between the output table names of a single MapReduce job and small integer ids, so that {@link BulkIngestKey} can carry a table
 * id rather than a table name through the map output.
 * <p>
 * A bulk ingest job writes every map output record with its destination table name spelled out - {@code shard}, {@code shardIndex}, {@code shardReverseIndex},
 * {@code DatawaveMetadata} - which costs a length prefix plus the name on every one of the millions of records a mapper spills, and makes the first (and most
 * often decisive) field of every sort comparison a multi byte {@code compareBytes}. The set of tables a job can write to is fixed before the job is submitted
 * and is already published in the job {@link Configuration} under {@link TableConfigurationUtil#JOB_OUTPUT_TABLE_NAMES}, so both sides of the shuffle can
 * derive the identical dictionary from the job configuration alone and exchange a one byte id instead.
 *
 * <h2>Ids preserve the existing sort order</h2>
 *
 * Ids are assigned in ascending {@link Text} order of the table name, which is the same unsigned-UTF-8 byte order {@code Text.compareTo} and
 * {@code WritableComparator.compareBytes} impose. Comparing ids therefore yields exactly the same relative order as comparing the names they stand for, and a
 * job whose tables are all in the dictionary sorts, groups, and partitions precisely as it did before the encoding existed. This matters because the reducer
 * relies on all records for one table arriving contiguously ({@link MultiRFileOutputFormatter} opens one RFile writer per table) and because RFiles require
 * their keys in Accumulo {@code Key} order within a table.
 *
 * <h2>Tables that are not in the dictionary</h2>
 *
 * A table absent from the dictionary - because a job never populated {@link TableConfigurationUtil#JOB_OUTPUT_TABLE_NAMES}, because the dictionary is disabled
 * by {@link #DICTIONARY_ENABLED}, or because a handler emitted a table nobody declared - takes {@link #UNKNOWN_ID} and its name is written inline exactly as it
 * always was. {@link #compareIds(int, int)} sorts every such table after every known one, so the order stays total and consistent whichever form a given record
 * happens to use. This is what makes the encoding safe to adopt incrementally: a job that never installs a dictionary behaves as it did before, one byte per
 * record larger.
 *
 * <h2>Installation and consistency</h2>
 *
 * The dictionary is installed per JVM rather than carried on each key, because the two methods that need it - {@link BulkIngestKey#write(java.io.DataOutput)}
 * and {@link BulkIngestKey#readFields(java.io.DataInput)} - are handed only a stream. {@link BulkIngestKey.Comparator#setConf(Configuration)} is the sole
 * installation point: MapReduce resolves the map output key's {@code RawComparator} through {@code WritableComparator.get(Class, Configuration)}, which pushes
 * the job configuration into the registered comparator, and it does so while building the map output collector (before the mapper emits its first record) and
 * again while building the reduce side merge (before the first record is deserialized). A single hook therefore covers both JVMs, and covers them early enough
 * that no record is ever serialized before the dictionary is in place.
 * <p>
 * Installing only there is deliberate. Every record a JVM writes must use the same dictionary as every other record it writes, or two records bound for the
 * same table could disagree on their table component and the reducer would see that table split across two groups. Hooking anything that runs mid-stream - the
 * {@code Writable} deserializer, a context writer, a reducer's {@code setup} - would risk exactly that, so it is not done.
 * <p>
 * Because both JVMs derive the dictionary from the same job configuration, they always agree. If a reader nonetheless meets an id it cannot resolve,
 * {@link BulkIngestKey#readFields(java.io.DataInput)} fails loudly rather than guessing a table name.
 *
 * @see BulkIngestKey
 */
final class BulkIndexKeyTableLookup {

    private static final Logger log = Logger.getLogger(BulkIndexKeyTableLookup.class);

    /**
     * Set to false in the job configuration to write table names inline as before. The encoding is job-transient - it appears only in map output and spill
     * files, never in an RFile or an Accumulo table - so this may be flipped per job without regard for any data already ingested.
     */
    public static final String DICTIONARY_ENABLED = "ingest.bulk.table.dictionary.enabled";

    /** the id of a table the dictionary does not know, whose name is written inline and which sorts after every known table */
    public static final int UNKNOWN_ID = -1;

    /** the dictionary in force when no job configuration has been seen: every table is unknown, so behavior matches the pre-dictionary encoding */
    public static final BulkIndexKeyTableLookup EMPTY = new BulkIndexKeyTableLookup(null, Collections.emptySet(), "");

    private static volatile BulkIndexKeyTableLookup installed = EMPTY;

    private final Configuration conf;

    /** table name by id; index i holds the name of table i */
    private final Text[] names;

    /** id by table name */
    private final Map<Text,Integer> ids;

    /**
     * The raw {@link TableConfigurationUtil#JOB_OUTPUT_TABLE_NAMES} value this dictionary was built from, used to recognize a repeat call to
     * {@link #configure(Configuration)} for a job that is already installed.
     */
    private final String signature;

    /**
     * The id {@link #idFor(Text)} resolved most recently, kept as a one entry memo.
     * <p>
     * Resolutions arrive in runs on the same table - a handler emits an event key, then its field index entries, then its global index entries - so most ask
     * for the table the previous one asked for. Skipping the map probe on those is worth it because the probe is keyed by {@link Text}: {@code Text.hashCode}
     * is not memoized, so it walks every byte of the name to hash it and then walks them again in {@code equals}. The memo replaces that with a single
     * {@code Text.equals} against the name the id stands for, which rejects a different table on the length comparison it starts with.
     * <p>
     * Only the id is stored, and it is validated against {@link #names} before being trusted. That is what lets this be a plain {@code int} with no
     * synchronization: an {@code int} write cannot tear, and only ids this dictionary assigned are ever stored, so every value this field can hold names a
     * valid entry of {@code names} and is either rejected by the equality check or is genuinely the id of the name being resolved. A race between threads costs
     * a missed memo, never a wrong id. Caching the name alongside the id in a second field would not be safe this way - a reader could pair one thread's name
     * with another thread's id. Because the memo lives on the dictionary instance, installing a new dictionary starts with a fresh memo rather than one that
     * must be re-validated against another dictionary's names.
     */
    private int lastResolvedId = 0;

    private BulkIndexKeyTableLookup(Configuration conf, Collection<String> tableNames, String signature) {
        this.conf = conf;
        this.signature = signature;

        // ascending Text order, so that comparing ids orders tables exactly as comparing their names would
        this.names = tableNames.stream().map(Text::new).sorted().toArray(Text[]::new);

        this.ids = new HashMap<>(Math.max(4, (int) (names.length / 0.75f) + 1));
        for (int id = 0; id < names.length; id++) {
            ids.put(names[id], id);
        }
    }

    /**
     * Build the dictionary for the given job configuration and install it for this JVM, unless a dictionary built from the same configuration is already
     * installed. Repeat calls with an equivalent configuration are free.
     *
     * @param conf
     *            the job configuration, or null to leave the installed dictionary alone
     */
    public static void configure(Configuration conf) {
        if (null == conf) {
            return;
        }

        if (!conf.getBoolean(DICTIONARY_ENABLED, true)) {
            if (installed != EMPTY) {
                log.info("Bulk ingest table name dictionary disabled by " + DICTIONARY_ENABLED + "; table names will be written inline");
                installed = EMPTY;
            }
            return;
        }

        String signature = conf.get(TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES, "");
        if (installed.signature.equals(signature)) {
            return;
        }

        BulkIndexKeyTableLookup dictionary = new BulkIndexKeyTableLookup(conf, TableConfigurationUtil.getJobOutputTableNames(conf), signature);
        installed = dictionary;

        if (log.isInfoEnabled()) {
            log.info("Bulk ingest table name dictionary installed with " + dictionary.size() + " tables: " + Arrays.toString(dictionary.names));
        }
    }

    /**
     * The dictionary installed for this JVM, never null. Returns {@link #EMPTY} until {@link #configure(Configuration)} has seen a job configuration naming at
     * least one output table.
     *
     * @return the installed dictionary
     */
    public static BulkIndexKeyTableLookup get() {
        return installed;
    }

    /**
     * Install a dictionary directly, bypassing the job configuration. Intended for tests; production installs through {@link #configure(Configuration)}.
     *
     * @param tableNames
     *            the output tables of the job being simulated
     * @return the installed dictionary
     */
    public static BulkIndexKeyTableLookup install(Collection<String> tableNames) {
        installed = new BulkIndexKeyTableLookup(null, tableNames, String.join(",", tableNames));
        return installed;
    }

    /** Restore the {@link #EMPTY} dictionary, so that table names are written inline. Intended for test teardown. */
    public static void reset() {
        installed = EMPTY;
    }

    /**
     * The id standing for the given table name, with the most recently resolved table memoized - see {@link #lastResolvedId}. Resolving the same table
     * repeatedly, which is how resolutions actually arrive, costs one short {@code Text.equals} rather than a map probe that hashes the whole name.
     *
     * @param tableName
     *            the table name, which may be null
     * @return the table's id, or {@link #UNKNOWN_ID} if this dictionary does not contain it
     */
    public int idFor(Text tableName) {
        // the empty check keeps the fallback dictionary free: EMPTY would otherwise hash the whole name just to probe a map with nothing in it
        if (null == tableName || names.length == 0) {
            return UNKNOWN_ID;
        }

        // read the memo once into a local, so the name it is validated against is the name of the id we return
        int last = lastResolvedId;
        if (names[last].equals(tableName)) {
            return last;
        }

        Integer id = ids.get(tableName);
        if (null == id) {
            // an unknown table has no id to remember, and leaving the memo alone keeps it useful for the resolutions on either side of it
            return UNKNOWN_ID;
        }
        lastResolvedId = id;
        return id;
    }

    /**
     * The table name an id stands for.
     *
     * @param id
     *            the id, as read from a serialized {@link BulkIngestKey}
     * @return the shared {@link Text} for that table, which callers must not mutate, or null if this dictionary has no such id
     */
    public Text nameFor(int id) {
        return (id < 0 || id >= names.length) ? null : names[id];
    }

    /**
     * The number of tables in this dictionary, which is also one past its largest id.
     *
     * @return the table count
     */
    public int size() {
        return names.length;
    }

    /**
     * The configuration this dictionary was built from.
     *
     * @return the job configuration, or null for {@link #EMPTY} and for dictionaries installed directly by a test
     */
    public Configuration getConf() {
        return conf;
    }

    /**
     * Order two table ids as their table names would order. Known ids compare by value - which is name order, since ids are assigned in name order - and
     * {@link #UNKNOWN_ID} sorts after every known id, leaving the caller to break the remaining tie on the inline names.
     *
     * @param id1
     *            the left table id
     * @param id2
     *            the right table id
     * @return a negative number, zero, or a positive number as the left table sorts before, with, or after the right
     */
    public static int compareIds(int id1, int id2) {
        return Integer.compare(id1 < 0 ? Integer.MAX_VALUE : id1, id2 < 0 ? Integer.MAX_VALUE : id2);
    }
}
