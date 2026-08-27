package datawave.ingest.mapreduce.job;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

/**
 * JVM-wide, effectively-immutable lookup from {@code (table, column family)} to a locality-group ordinal, used to sort and emit {@link BulkIngestKey}s by
 * locality group during bulk ingest so that {@code MultiRFileOutputFormatter} can write contiguous, sorted locality-group runs per RFile.
 * <p>
 * Ordinals for a table are assigned {@code 0..n-1} in ascending locality-group-name order; the default (unnamed) group is always ordinal {@code n}. A table
 * that is not opted in (see {@link TableConfigurationUtil#JOB_OUTPUT_LOCALITY_GROUP_TABLES}), or that has no enabled locality groups configured, is not
 * "enabled" in this lookup and every column family for it resolves to ordinal {@code 0} &mdash; i.e. the sort order collapses to today's order exactly.
 * <p>
 * A single instance is built once per distinct job configuration and installed as the JVM-wide instance via {@link #configure(Configuration)}; callers read the
 * current instance via {@link #get()}. Once built, an instance is immutable other than a small internal memo cache (see
 * {@link #ordinalFor(Text, ByteSequence)}) that is safe to read/write concurrently.
 */
public final class BulkIngestKeyLocalityGroupLookup {

    /** Sentinel signature used by {@link #install(Map)} so a later {@link #configure(Configuration)} call always rebuilds from the given configuration. */
    private static final String INSTALLED_SIGNATURE = "\0<installed>\0";

    private static final BulkIngestKeyLocalityGroupLookup EMPTY = new BulkIngestKeyLocalityGroupLookup(Collections.emptyMap());

    private static final Object BUILD_LOCK = new Object();

    private static volatile BulkIngestKeyLocalityGroupLookup instance = EMPTY;
    private static volatile String signature = null;

    private final Map<Text,TableGroups> tablesByName;
    private final AtomicReference<Memo> memo = new AtomicReference<>(Memo.EMPTY);

    private BulkIngestKeyLocalityGroupLookup(Map<Text,TableGroups> tablesByName) {
        this.tablesByName = tablesByName;
    }

    /**
     * Build (once per JVM, per distinct configuration) and install the JVM-wide lookup from the given job configuration, then return it. A repeat call with a
     * configuration that produces the same signature (opt-in table list + each opted-in table's serialized table configuration) is a no-op and returns the
     * already-built instance.
     *
     * @param conf
     *            the hadoop configuration
     * @return the JVM-wide instance for this configuration
     */
    public static BulkIngestKeyLocalityGroupLookup configure(Configuration conf) {
        String sig = buildSignature(conf);
        if (sig.equals(signature)) {
            return instance;
        }
        synchronized (BUILD_LOCK) {
            if (sig.equals(signature)) {
                return instance;
            }
            BulkIngestKeyLocalityGroupLookup built = build(conf);
            instance = built;
            signature = sig;
            return built;
        }
    }

    /**
     * @return the current JVM-wide instance, or an empty lookup if {@link #configure(Configuration)} has never been called
     */
    public static BulkIngestKeyLocalityGroupLookup get() {
        return instance;
    }

    /**
     * Test seam: install an instance built directly from {@code table -> groupName -> columnFamilies}, bypassing {@link TableConfigurationUtil} and
     * {@link Configuration} entirely. A subsequent call to {@link #configure(Configuration)} always rebuilds (the installed instance never satisfies the no-op
     * signature check).
     *
     * @param tableToGroupToFamilies
     *            table name -&gt; locality group name -&gt; column families in that group
     * @return the installed instance
     */
    public static BulkIngestKeyLocalityGroupLookup install(Map<String,Map<String,Set<Text>>> tableToGroupToFamilies) {
        Map<Text,TableGroups> tables = new HashMap<>();
        if (tableToGroupToFamilies != null) {
            for (Map.Entry<String,Map<String,Set<Text>>> entry : tableToGroupToFamilies.entrySet()) {
                SortedMap<String,Set<Text>> sorted = new TreeMap<>(entry.getValue());
                if (!sorted.isEmpty()) {
                    tables.put(new Text(entry.getKey()), TableGroups.build(sorted));
                }
            }
        }
        BulkIngestKeyLocalityGroupLookup built = tables.isEmpty() ? EMPTY : new BulkIngestKeyLocalityGroupLookup(tables);
        synchronized (BUILD_LOCK) {
            instance = built;
            signature = INSTALLED_SIGNATURE;
        }
        return built;
    }

    /** Reset the JVM-wide instance to empty. Primarily for tests. */
    public static void reset() {
        synchronized (BUILD_LOCK) {
            instance = EMPTY;
            signature = null;
        }
    }

    private static String buildSignature(Configuration conf) {
        Set<String> optInTables = new TreeSet<>(TableConfigurationUtil.getLocalityGroupTables(conf));
        StringBuilder sb = new StringBuilder(64);
        sb.append(conf.get(TableConfigurationUtil.JOB_OUTPUT_LOCALITY_GROUP_TABLES, ""));
        for (String table : optInTables) {
            sb.append('\0').append(table).append('=').append(conf.get(table + TableConfigurationUtil.TABLE_CONFIGURATION_PROPERTY, ""));
        }
        return sb.toString();
    }

    private static BulkIngestKeyLocalityGroupLookup build(Configuration conf) {
        Set<String> optInTables = new TreeSet<>(TableConfigurationUtil.getLocalityGroupTables(conf));
        if (optInTables.isEmpty()) {
            return EMPTY;
        }

        TableConfigurationUtil tcu = new TableConfigurationUtil(conf);
        Map<Text,TableGroups> tables = new HashMap<>();
        for (String tableName : optInTables) {
            try {
                Map<String,String> properties = tcu.getTableProperties(tableName);
                if (null == properties || properties.isEmpty()) {
                    continue;
                }

                Set<String> enabledGroupNames = splitCsv(properties.get(Property.TABLE_LOCALITY_GROUPS.getKey()));
                if (enabledGroupNames.isEmpty()) {
                    continue;
                }

                Map<String,Set<Text>> groups = tcu.getLocalityGroups(tableName);
                SortedMap<String,Set<Text>> enabledGroups = new TreeMap<>();
                for (Map.Entry<String,Set<Text>> entry : groups.entrySet()) {
                    if (enabledGroupNames.contains(entry.getKey())) {
                        enabledGroups.put(entry.getKey(), entry.getValue());
                    }
                }

                if (!enabledGroups.isEmpty()) {
                    tables.put(new Text(tableName), TableGroups.build(enabledGroups));
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Unable to resolve locality groups for table " + tableName, e);
            }
        }

        return tables.isEmpty() ? EMPTY : new BulkIngestKeyLocalityGroupLookup(tables);
    }

    private static Set<String> splitCsv(String value) {
        Set<String> result = new HashSet<>();
        if (null != value) {
            for (String part : value.split(",")) {
                String trimmed = part.trim();
                if (!trimmed.isEmpty()) {
                    result.add(trimmed);
                }
            }
        }
        return result;
    }

    /**
     * @return {@code true} if no table is opted in (or opted-in but not actually configured with any enabled locality group); when {@code true},
     *         {@link #ordinalFor(Text, ByteSequence)} always returns {@code 0} without touching the table map or the memo
     */
    public boolean isEmpty() {
        return tablesByName.isEmpty();
    }

    /**
     * @param table
     *            the output table name
     * @return {@code true} if the table has at least one enabled, opted-in locality group
     */
    public boolean isEnabled(Text table) {
        return tablesByName.containsKey(table);
    }

    /**
     * @param table
     *            the output table name
     * @return the default (unnamed) locality group's ordinal for the table, i.e. the number of named groups {@code n}; {@code 0} if the table is not enabled
     */
    public int defaultOrdinal(Text table) {
        TableGroups groups = tablesByName.get(table);
        return null != groups ? groups.defaultOrdinal : 0;
    }

    /**
     * Resolve the locality-group ordinal for a key's table and column family. Fast paths: {@link #isEmpty()} short-circuits to {@code 0} without touching
     * anything; otherwise a small memo (safe for concurrent use) avoids the {@code HashMap} probe when consecutive calls repeat the same table and/or the same
     * column family, which is the common case since keys arrive in per-table, often per-column-family, runs.
     *
     * @param table
     *            the output table name
     * @param cf
     *            the key's column family, e.g. from {@code Key.getColumnFamilyData()}
     * @return the ordinal, {@code 0..n-1} for a named group or {@code n} (the default ordinal) for anything else, including a non-enabled table
     */
    public int ordinalFor(Text table, ByteSequence cf) {
        if (isEmpty()) {
            return 0;
        }

        Memo m = memo.get();
        TableGroups groups;
        if (null != m.table && m.table.equals(table)) {
            groups = m.groups;
        } else {
            groups = tablesByName.get(table);
            m = new Memo(new Text(table), groups, null, 0);
            memo.set(m);
        }

        if (null == groups) {
            return 0;
        }

        if (null != m.cf && m.cf.equals(cf)) {
            return m.ordinal;
        }

        int ordinal = groups.ordinalFor(cf);
        // copy the cf: the caller's ByteSequence typically wraps a Key's internal array, which we must not hold on to
        memo.set(new Memo(m.table, groups, new ArrayByteSequence(cf.toArray()), ordinal));
        return ordinal;
    }

    /**
     * @param table
     *            the output table name
     * @param ordinal
     *            a value previously returned by {@link #ordinalFor(Text, ByteSequence)} (or {@link #defaultOrdinal(Text)}) for this table
     * @return the locality group's name, or {@code null} if {@code ordinal} is the default ordinal for this table
     * @throws IllegalArgumentException
     *             if {@code ordinal} is neither a valid named-group ordinal nor the default ordinal for this table
     */
    public String groupName(Text table, int ordinal) {
        TableGroups groups = tablesByName.get(table);
        int defaultOrdinal = null != groups ? groups.defaultOrdinal : 0;
        if (ordinal == defaultOrdinal) {
            return null;
        }
        if (null == groups || ordinal < 0 || ordinal >= groups.groupName.length) {
            throw new IllegalArgumentException("No locality group ordinal " + ordinal + " for table " + table);
        }
        return groups.groupName[ordinal];
    }

    /**
     * @param table
     *            the output table name
     * @param ordinal
     *            a value previously returned by {@link #ordinalFor(Text, ByteSequence)} (or {@link #defaultOrdinal(Text)}) for this table
     * @return the column families making up the named locality group, or {@code null} if {@code ordinal} is the default ordinal for this table
     * @throws IllegalArgumentException
     *             if {@code ordinal} is neither a valid named-group ordinal nor the default ordinal for this table
     */
    public Set<ByteSequence> groupFamilies(Text table, int ordinal) {
        TableGroups groups = tablesByName.get(table);
        int defaultOrdinal = null != groups ? groups.defaultOrdinal : 0;
        if (ordinal == defaultOrdinal) {
            return null;
        }
        if (null == groups || ordinal < 0 || ordinal >= groups.groupFamilies.length) {
            throw new IllegalArgumentException("No locality group ordinal " + ordinal + " for table " + table);
        }
        return groups.groupFamilies[ordinal];
    }

    /** Immutable per-table ordinal assignment. */
    private static final class TableGroups {
        private final Map<ByteSequence,Integer> cfToOrdinal;
        private final int minCfLength;
        private final int maxCfLength;
        private final String[] groupName;
        private final Set<ByteSequence>[] groupFamilies;
        private final int defaultOrdinal;

        private TableGroups(Map<ByteSequence,Integer> cfToOrdinal, int minCfLength, int maxCfLength, String[] groupName, Set<ByteSequence>[] groupFamilies) {
            this.cfToOrdinal = cfToOrdinal;
            this.minCfLength = minCfLength;
            this.maxCfLength = maxCfLength;
            this.groupName = groupName;
            this.groupFamilies = groupFamilies;
            this.defaultOrdinal = groupName.length;
        }

        int ordinalFor(ByteSequence cf) {
            int len = cf.length();
            if (len < minCfLength || len > maxCfLength) {
                return defaultOrdinal;
            }
            Integer ordinal = cfToOrdinal.get(cf);
            return null != ordinal ? ordinal : defaultOrdinal;
        }

        @SuppressWarnings("unchecked")
        static TableGroups build(SortedMap<String,Set<Text>> groupsByNameAscending) {
            int n = groupsByNameAscending.size();
            String[] names = new String[n];
            Set<ByteSequence>[] families = new Set[n];
            Map<ByteSequence,Integer> cfToOrdinal = new HashMap<>();
            int minLen = Integer.MAX_VALUE;
            int maxLen = 0;

            int ordinal = 0;
            for (Map.Entry<String,Set<Text>> entry : groupsByNameAscending.entrySet()) {
                names[ordinal] = entry.getKey();
                Set<ByteSequence> family = new HashSet<>();
                for (Text cf : entry.getValue()) {
                    ByteSequence bs = new ArrayByteSequence(Arrays.copyOf(cf.getBytes(), cf.getLength()));
                    family.add(bs);
                    cfToOrdinal.put(bs, ordinal);
                    minLen = Math.min(minLen, bs.length());
                    maxLen = Math.max(maxLen, bs.length());
                }
                families[ordinal] = Collections.unmodifiableSet(family);
                ordinal++;
            }

            if (cfToOrdinal.isEmpty()) {
                minLen = 0;
                maxLen = 0;
            }

            return new TableGroups(cfToOrdinal, minLen, maxLen, names, families);
        }
    }

    /** Immutable memo entry, swapped atomically so reads/writes from multiple threads are safe (a stale read just costs a redundant lookup). */
    private static final class Memo {
        static final Memo EMPTY = new Memo(null, null, null, 0);

        final Text table;
        final TableGroups groups;
        final ByteSequence cf;
        final int ordinal;

        Memo(Text table, TableGroups groups, ByteSequence cf, int ordinal) {
            this.table = table;
            this.groups = groups;
            this.cf = cf;
            this.ordinal = ordinal;
        }
    }
}
