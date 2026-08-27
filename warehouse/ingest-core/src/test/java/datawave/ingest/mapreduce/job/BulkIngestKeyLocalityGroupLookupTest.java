package datawave.ingest.mapreduce.job;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.util.LocalityGroupUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.ingest.config.TableConfigCache;
import datawave.ingest.data.config.ingest.AccumuloHelper;

/**
 * Exercises {@link BulkIngestKeyLocalityGroupLookup} end to end (via a serialized/deserialized table config, as {@code TableConfigurationUtil} produces during
 * a real job) and through the {@link BulkIngestKeyLocalityGroupLookup#install(Map)} test seam for the parts that don't need a full table config round trip.
 */
public class BulkIngestKeyLocalityGroupLookupTest {

    private static final String FULLCONTENT = "fullcontent";
    private static final String TERMFREQUENCY = "termfrequency";

    @BeforeEach
    public void resetStatics() throws NoSuchFieldException, IllegalAccessException {
        BulkIngestKeyLocalityGroupLookup.reset();

        // TableConfigCache is itself a JVM-wide singleton (see getCurrentCache); reset it between tests the same way
        // datawave.ingest.csv.TableConfigurationUtilTest does, so each test's serialized table config is actually re-read.
        Field cache = TableConfigCache.class.getDeclaredField("cache");
        cache.setAccessible(true);
        cache.set(null, null);
    }

    private static ByteSequence bs(String s) {
        return new ArrayByteSequence(s.getBytes(StandardCharsets.UTF_8));
    }

    private static Configuration baseConf() {
        Configuration conf = new Configuration(false);
        conf.set(AccumuloHelper.USERNAME, "root");
        conf.set(AccumuloHelper.PASSWORD, Base64.getEncoder().encodeToString("pass".getBytes(StandardCharsets.UTF_8)));
        conf.set(AccumuloHelper.INSTANCE_NAME, "instance");
        conf.set(AccumuloHelper.ZOOKEEPERS, "localhost:2181");
        return conf;
    }

    /** Encode a table's properties exactly as {@code TableConfigurationUtil#serializeTableConfgurationIntoConf} does, and register it as a job output table. */
    private static void setTableConfig(Configuration conf, String table, Map<String,String> props) throws IOException {
        String existing = conf.get(TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES);
        conf.set(TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES, null == existing ? table : existing + "," + table);

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(new HashMap<>(props));
        }
        conf.set(table + TableConfigurationUtil.TABLE_CONFIGURATION_PROPERTY, Base64.getEncoder().encodeToString(baos.toByteArray()));
    }

    private static Map<String,String> shardLikeProperties(String enabledGroups) {
        Map<String,String> props = new HashMap<>();
        props.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + FULLCONTENT, "d");
        props.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + TERMFREQUENCY, "tf");
        props.put(Property.TABLE_LOCALITY_GROUPS.getKey(), enabledGroups);
        return props;
    }

    @Test
    public void testOrdinalsFromSerializedTableConfigRoundTrip() throws IOException {
        Configuration conf = baseConf();
        setTableConfig(conf, "shard", shardLikeProperties(FULLCONTENT + "," + TERMFREQUENCY));
        conf.set(TableConfigurationUtil.JOB_OUTPUT_LOCALITY_GROUP_TABLES, "shard");

        BulkIngestKeyLocalityGroupLookup lookup = BulkIngestKeyLocalityGroupLookup.configure(conf);
        Text shard = new Text("shard");

        assertFalse(lookup.isEmpty());
        assertTrue(lookup.isEnabled(shard));
        assertEquals(2, lookup.defaultOrdinal(shard));

        // ascending LG-name order: fullcontent < termfrequency
        assertEquals(0, lookup.ordinalFor(shard, bs("d")));
        assertEquals(1, lookup.ordinalFor(shard, bs("tf")));
        // anything else (fi\0FIELD, datatype\0uid, ...) falls to the default ordinal
        assertEquals(2, lookup.ordinalFor(shard, bs("fi\0FIELD")));
        assertEquals(2, lookup.ordinalFor(shard, bs("datatype\0uid")));

        assertEquals(FULLCONTENT, lookup.groupName(shard, 0));
        assertEquals(TERMFREQUENCY, lookup.groupName(shard, 1));
        assertNull(lookup.groupName(shard, 2));

        assertTrue(lookup.groupFamilies(shard, 0).contains(bs("d")));
        assertTrue(lookup.groupFamilies(shard, 1).contains(bs("tf")));
        assertNull(lookup.groupFamilies(shard, 2));
    }

    @Test
    public void testTableGroupsEnabledFiltersOutNonEnabledGroups() throws IOException {
        Configuration conf = baseConf();
        // both fullcontent and termfrequency are configured on the table, but only fullcontent is in table.groups.enabled
        setTableConfig(conf, "shard", shardLikeProperties(FULLCONTENT));
        conf.set(TableConfigurationUtil.JOB_OUTPUT_LOCALITY_GROUP_TABLES, "shard");

        BulkIngestKeyLocalityGroupLookup lookup = BulkIngestKeyLocalityGroupLookup.configure(conf);
        Text shard = new Text("shard");

        assertTrue(lookup.isEnabled(shard));
        assertEquals(1, lookup.defaultOrdinal(shard));
        assertEquals(0, lookup.ordinalFor(shard, bs("d")));
        // tf is configured but not enabled -> Accumulo would ignore it too, so it must fall to the default ordinal
        assertEquals(1, lookup.ordinalFor(shard, bs("tf")));
    }

    @Test
    public void testOptInListGatesParticipation() throws IOException {
        Configuration conf = baseConf();
        setTableConfig(conf, "shard", shardLikeProperties(FULLCONTENT));
        // note: ingest.bulk.locality.groups.tables intentionally left unset

        BulkIngestKeyLocalityGroupLookup notOptedIn = BulkIngestKeyLocalityGroupLookup.configure(conf);
        Text shard = new Text("shard");
        assertTrue(notOptedIn.isEmpty());
        assertFalse(notOptedIn.isEnabled(shard));
        assertEquals(0, notOptedIn.ordinalFor(shard, bs("d")));

        // opt shard in, and also opt in a table that isn't actually configured anywhere -- must degrade gracefully
        conf.set(TableConfigurationUtil.JOB_OUTPUT_LOCALITY_GROUP_TABLES, "shard,bogusTable");
        BulkIngestKeyLocalityGroupLookup optedIn = BulkIngestKeyLocalityGroupLookup.configure(conf);
        assertFalse(optedIn.isEmpty());
        assertTrue(optedIn.isEnabled(shard));
        Text bogus = new Text("bogusTable");
        assertFalse(optedIn.isEnabled(bogus));
        assertEquals(0, optedIn.ordinalFor(bogus, bs("d")));
    }

    @Test
    public void testNoConfigurationYieldsAllZero() {
        Configuration conf = new Configuration(false);

        BulkIngestKeyLocalityGroupLookup lookup = BulkIngestKeyLocalityGroupLookup.configure(conf);
        Text anyTable = new Text("anyTable");

        assertTrue(lookup.isEmpty());
        assertFalse(lookup.isEnabled(anyTable));
        assertEquals(0, lookup.defaultOrdinal(anyTable));
        assertEquals(0, lookup.ordinalFor(anyTable, bs("anything")));
    }

    @Test
    public void testColumnFamilyLengthGate() {
        Map<String,Set<Text>> groups = new HashMap<>();
        groups.put(FULLCONTENT, Set.of(new Text("d"))); // length 1
        groups.put(TERMFREQUENCY, Set.of(new Text("tf"))); // length 2

        BulkIngestKeyLocalityGroupLookup lookup = BulkIngestKeyLocalityGroupLookup.install(Map.of("shard", groups));
        Text shard = new Text("shard");

        assertEquals(0, lookup.ordinalFor(shard, bs("d")));
        assertEquals(1, lookup.ordinalFor(shard, bs("tf")));

        // below the min configured length (0 < 1) -> default ordinal via the length gate
        assertEquals(2, lookup.ordinalFor(shard, bs("")));
        // within [min,max] but not a configured cf -> default ordinal via a map miss
        assertEquals(2, lookup.ordinalFor(shard, bs("e")));
        assertEquals(2, lookup.ordinalFor(shard, bs("zz")));
        // above the max configured length (12 > 2) -> default ordinal via the length gate
        assertEquals(2, lookup.ordinalFor(shard, bs("datatype\0uid")));
    }

    @Test
    public void testMemoCorrectnessAcrossTableAndCfSwitches() {
        Map<String,Set<Text>> shardGroups = new HashMap<>();
        shardGroups.put(FULLCONTENT, Set.of(new Text("d")));
        shardGroups.put(TERMFREQUENCY, Set.of(new Text("tf")));

        Map<String,Set<Text>> otherGroups = new HashMap<>();
        otherGroups.put("g1", Set.of(new Text("x")));

        BulkIngestKeyLocalityGroupLookup lookup = BulkIngestKeyLocalityGroupLookup.install(Map.of("shard", shardGroups, "other", otherGroups));

        Text shard = new Text("shard");
        Text other = new Text("other");

        assertEquals(0, lookup.ordinalFor(shard, bs("d"))); // table miss -> resolve and memoize
        assertEquals(0, lookup.ordinalFor(shard, bs("d"))); // table hit, cf hit (memo)
        assertEquals(1, lookup.ordinalFor(shard, bs("tf"))); // table hit, cf switch
        assertEquals(0, lookup.ordinalFor(other, bs("x"))); // table switch
        assertEquals(1, lookup.ordinalFor(other, bs("y"))); // other table's default ordinal
        assertEquals(0, lookup.ordinalFor(shard, bs("d"))); // switch back to shard, must re-resolve correctly
        assertEquals(1, lookup.ordinalFor(shard, bs("tf")));
    }

    @Test
    public void testMemoHandlesTableTextMutatedInPlace() {
        Map<String,Set<Text>> shardGroups = new HashMap<>();
        shardGroups.put(FULLCONTENT, Set.of(new Text("d")));

        Map<String,Set<Text>> otherGroups = new HashMap<>();
        otherGroups.put("g1", Set.of(new Text("x")));

        BulkIngestKeyLocalityGroupLookup lookup = BulkIngestKeyLocalityGroupLookup.install(Map.of("shard", shardGroups, "other", otherGroups));

        // BulkIngestKey#setTableName mutates the same Text instance in place rather than allocating a new one;
        // the memo must not mistake the mutated object for the table it previously resolved.
        Text reused = new Text("shard");
        assertEquals(0, lookup.ordinalFor(reused, bs("d")));

        reused.set("other");
        assertEquals(0, lookup.ordinalFor(reused, bs("x")));
        assertEquals(1, lookup.ordinalFor(reused, bs("nope")));
    }

    @Test
    public void testConfigureIsIdempotentAndResetClears() throws IOException {
        Configuration conf = baseConf();
        setTableConfig(conf, "shard", shardLikeProperties(FULLCONTENT));
        conf.set(TableConfigurationUtil.JOB_OUTPUT_LOCALITY_GROUP_TABLES, "shard");

        BulkIngestKeyLocalityGroupLookup first = BulkIngestKeyLocalityGroupLookup.configure(conf);
        BulkIngestKeyLocalityGroupLookup second = BulkIngestKeyLocalityGroupLookup.configure(conf);
        assertSame(first, second, "repeat configure() with an unchanged conf must be a no-op");
        assertSame(first, BulkIngestKeyLocalityGroupLookup.get());

        // changing the opt-in list changes the signature -> configure() must rebuild
        conf.set(TableConfigurationUtil.JOB_OUTPUT_LOCALITY_GROUP_TABLES, "");
        BulkIngestKeyLocalityGroupLookup third = BulkIngestKeyLocalityGroupLookup.configure(conf);
        assertNotSame(first, third);
        assertTrue(third.isEmpty());

        BulkIngestKeyLocalityGroupLookup.reset();
        assertTrue(BulkIngestKeyLocalityGroupLookup.get().isEmpty());
        assertSame(BulkIngestKeyLocalityGroupLookup.get(), BulkIngestKeyLocalityGroupLookup.get());
    }

    /**
     * Accumulo stores {@code table.group.<name>} values via {@link LocalityGroupUtil#encodeColumnFamilies(Set)}, which escapes {@code ,}, {@code \}, and
     * non-printable/non-ASCII bytes as {@code \xNN}. {@code TableConfigurationUtil#getLocalityGroups} must decode those values with
     * {@link LocalityGroupUtil#decodeColumnFamilies(String)} rather than a raw {@code split(",")}, or escaped families (like the shard table's real
     * {@code fi\0} field-index family) never match an actual key column family and silently fall through to the default group.
     */
    @Test
    public void testResolvesEscapedLocalityGroupFamilies() throws IOException {
        String FIELDINDEX = "fieldindex";
        String COMMAGROUP = "commagroup";

        Configuration conf = baseConf();
        Map<String,String> props = new HashMap<>();
        // "fi\0" encodes to "fi\x00" (non-printable byte); "a,b" encodes to "a\x2Cb" (escaped literal comma)
        props.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + FIELDINDEX, LocalityGroupUtil.encodeColumnFamilies(Set.of(new Text("fi\0"))));
        props.put(Property.TABLE_LOCALITY_GROUP_PREFIX.getKey() + COMMAGROUP, LocalityGroupUtil.encodeColumnFamilies(Set.of(new Text("a,b"))));
        props.put(Property.TABLE_LOCALITY_GROUPS.getKey(), FIELDINDEX + "," + COMMAGROUP);
        setTableConfig(conf, "shard", props);
        conf.set(TableConfigurationUtil.JOB_OUTPUT_LOCALITY_GROUP_TABLES, "shard");

        BulkIngestKeyLocalityGroupLookup lookup = BulkIngestKeyLocalityGroupLookup.configure(conf);
        Text shard = new Text("shard");

        assertTrue(lookup.isEnabled(shard));
        // ascending LG-name order: commagroup < fieldindex
        assertEquals(0, lookup.ordinalFor(shard, bs("a,b")));
        assertEquals(1, lookup.ordinalFor(shard, bs("fi\0")));
        assertEquals(2, lookup.defaultOrdinal(shard));

        assertTrue(lookup.groupFamilies(shard, 0).contains(bs("a,b")));
        assertTrue(lookup.groupFamilies(shard, 1).contains(bs("fi\0")));
    }
}
