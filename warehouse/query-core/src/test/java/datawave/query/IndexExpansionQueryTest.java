package datawave.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.net.URL;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.admin.SecurityOperations;
import org.apache.accumulo.core.client.admin.TableOperations;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.iterators.IteratorTimeoutException;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.exceptions.DatawaveQueryException;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.planner.DefaultQueryPlanner;
import datawave.query.planner.QueryPlanner;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.IndexExpansionIngest;
import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryStopwatch;
import datawave.query.util.TestIndexTableNames;
import datawave.test.MacTestUtil;
import datawave.util.TableName;

/**
 * A suite of tests that validate timeout-based index expansion. Other index expansion tests verify threshold based expansion.
 * <p>
 * Each base test case has the following eleven permutations
 * <ul>
 * <li>timeout on initial seek</li>
 * <li>timeout on next</li>
 * <li>NPE on seek</li>
 * <li>NPE on next</li>
 * <li>NPE on random operation</li>
 * <li>IteratorInterruptedException on seek</li>
 * <li>IteratorInterruptedException on next</li>
 * <li>IteratorInterruptedException on random operation</li>
 * <li>IteratorTimeoutException on seek</li>
 * <li>IteratorTimeoutException on next</li>
 * <li>IteratorTimeoutException on random operation</li>
 * </ul>
 */
@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = "datawave.query")
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class IndexExpansionQueryTest extends AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(IndexExpansionQueryTest.class);

    protected static final String PASSWORD = "password";
    protected static final Authorizations auths = new Authorizations("VIZ-A", "VIZ-B", "VIZ-C");
    protected static MiniAccumuloCluster mac;
    protected static AccumuloClient client = null;
    protected static TableOperations tops = null;

    // switch between MiniAccumuloCluster and InMemoryAccumulo
    private static final boolean useMAC = false;

    // switch between old code and new code
    private final boolean useNewIndexLookups = false;

    // 10 values per prefix, low expansion thresholds, low scan thresholds
    // using this tests can simulate exceptions or timeouts on initial seek vs. next calls
    private long expansionThreshold = 500;
    private final long scanThresholdMS = 50L;
    private final int iterationDelayMS = 10;
    private final int timeoutDelayMS = 30_000;

    private long expectedRegexExpansionTime = -1;
    private long expectedRangeExpansionTime = -1;

    @TempDir
    public static Path folder;

    @Autowired
    @Qualifier("EventQueryNoRules")
    protected ShardQueryLogic logic;

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    protected void extraConfigurations() {
        // no-op
    }

    @Override
    protected void extraAssertions() {
        QueryPlanner planner = logic.getQueryPlanner();
        assertInstanceOf(StageTimingDefaultQueryPlanner.class, planner);
        StageTimingDefaultQueryPlanner stagePlanner = (StageTimingDefaultQueryPlanner) planner;
        log.info("range expansion: {} ms", stagePlanner.getExpandRangeStage());
        log.info("regex expansion: {} ms", stagePlanner.getExpandRegexStage());

        if (expectedRangeExpansionTime >= 0) {
            assertRangeTime(expectedRangeExpansionTime);
        }

        if (expectedRegexExpansionTime >= 0) {
            assertRegexTime(expectedRegexExpansionTime);
        }
    }

    protected void assertRegexTime(long expected) {
        QueryPlanner planner = logic.getQueryPlanner();
        assertInstanceOf(StageTimingDefaultQueryPlanner.class, planner);
        StageTimingDefaultQueryPlanner stagePlanner = (StageTimingDefaultQueryPlanner) planner;

        if (stagePlanner.getExpandRegexStage() < expected) {
            // assume that faster is better, for now
            return;
        }

        String msg = "expected: " + expected + " but was " + stagePlanner.getExpandRegexStage();
        long delta = (long) Math.max((expected * 0.1), 250);
        assertEquals(expected, stagePlanner.getExpandRegexStage(), delta, msg);
    }

    protected void assertRangeTime(long expected) {
        QueryPlanner planner = logic.getQueryPlanner();
        assertInstanceOf(StageTimingDefaultQueryPlanner.class, planner);
        StageTimingDefaultQueryPlanner stagePlanner = (StageTimingDefaultQueryPlanner) planner;

        if (stagePlanner.getExpandRangeStage() < expected) {
            // assume that faster is better
            return;
        }

        String msg = "expected: " + expected + " but was " + stagePlanner.getExpandRangeStage();
        long delta = (long) Math.max((expected * 0.1), 250);
        assertEquals(expected, stagePlanner.getExpandRangeStage(), delta, msg);
    }

    protected void expectRegexExpansionTime(long time) {
        expectedRegexExpansionTime = time;
    }

    protected void expectRangeExpansionTime(long time) {
        expectedRangeExpansionTime = time;
    }

    public Authorizations getAuths() {
        return auths;
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        if (useMAC) {
            MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.toFile(), PASSWORD);
            cfg.setNumTservers(1);
            mac = new MiniAccumuloCluster(cfg);
            mac.start();

            client = mac.createAccumuloClient("root", new PasswordToken(PASSWORD));
        } else {
            // do not use a rebuilding scanner because the test requires exceptions to propagate
            // all the way back to the client.
            InMemoryInstance instance = new InMemoryInstance(IndexExpansionQueryTest.class.getName());
            client = new InMemoryAccumuloClient("root", instance);
        }

        // grant root user all auths so they can scan the tables
        SecurityOperations sops = client.securityOperations();
        sops.changeUserAuthorizations("root", auths);
        tops = client.tableOperations();

        IndexExpansionIngest.write(client, auths);

        IndexIngestUtil ingestUtil = new IndexIngestUtil();
        ingestUtil.write(client, auths);

        for (String tableName : indexTableNames()) {
            MacTestUtil.compactTable(tops, tableName);
        }
        MacTestUtil.compactTable(tops, TableName.METADATA);
    }

    private StageTimingDefaultQueryPlanner stageQueryPlanner;

    @BeforeEach
    public void beforeEach() {
        setClientForTest(client);

        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        Preconditions.checkNotNull(hadoopConfig);
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());

        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(folder.toUri().toString());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(config));

        logic.setUseNewIndexLookups(useNewIndexLookups);
        logic.getQueryPlanner().setRules(Collections.emptySet());

        logic.setMaxIndexScanTimeMillis(scanThresholdMS);
        logic.setMaxAnyFieldScanTimeMillis(scanThresholdMS);

        logic.setMaxUnfieldedExpansionThreshold((int) expansionThreshold);
        logic.setMaxValueExpansionThreshold((int) expansionThreshold);

        logic.setInitialMaxTermThreshold(1_000);
        logic.setIntermediateMaxTermThreshold(1_000);
        logic.setIndexedMaxTermThreshold(1_000);
        logic.setFinalMaxTermThreshold(1_000);

        stageQueryPlanner = new StageTimingDefaultQueryPlanner();
        logic.setQueryPlanner(stageQueryPlanner);

        givenDate("20250101", "20250105");
    }

    /**
     * Get all forward index table names along with the reverse index
     *
     * @return the set of index table names
     */
    protected static Set<String> indexTableNames() {
        Set<String> names = new HashSet<>(TestIndexTableNames.names());
        names.add(TableName.SHARD_RINDEX);
        return names;
    }

    @Test
    public void testFieldedRegexTimeoutOnInitialSeek() throws Exception {
        try {
            addDelayIterator(timeoutDelayMS);
            givenQuery("FIELD_A =~ 'a.*'");
            expectPlan("((_Value_ = true) && (FIELD_A =~ 'a.*'))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeDelayIterator();
        }
    }

    @Test
    public void testFieldedRegexTimeoutOnNext() throws Exception {
        try {
            addDelayIterator();
            givenQuery("FIELD_A =~ 'a.*'");
            expectPlan("((_Value_ = true) && (FIELD_A =~ 'a.*'))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeDelayIterator();
        }
    }

    @Test
    public void testFieldedRegexNPEOnSeek() throws Exception {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "seek");
            givenQuery("FIELD_A =~ 'a.*'");
            expectPlan("((_Value_ = true) && (FIELD_A =~ 'a.*'))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testFieldedRegexNPEOnNext() throws Exception {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "next");
            givenQuery("FIELD_A =~ 'a.*'");
            expectPlan("((_Value_ = true) && (FIELD_A =~ 'a.*'))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testFieldedRegexNPERandom() throws Exception {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "random");
            givenQuery("FIELD_A =~ 'a.*'");
            expectPlan("((_Value_ = true) && (FIELD_A =~ 'a.*'))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // IterationInterruptedException on seek
    @Test
    public void testFieldedRegexIIEOnInitialSeek() throws Exception {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IIE for test", "seek");
            givenQuery("FIELD_A =~ 'a.*'");
            expectPlan("((_Value_ = true) && (FIELD_A =~ 'a.*'))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // IterationInterruptedException on next
    @Test
    public void testFieldedRegexIIEOnNext() throws Exception {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IIE for test", "next");
            givenQuery("FIELD_A =~ 'a.*'");
            expectPlan("((_Value_ = true) && (FIELD_A =~ 'a.*'))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // IteratorTimeoutException on next
    @Test
    public void testFieldedRegexITEOnNext() throws Exception {
        try {
            addIOExceptionIterator(IteratorTimeoutException.class.getName(), "ITE for test", "next");
            givenQuery("FIELD_A =~ 'a.*'");
            expectPlan("((_Value_ = true) && (FIELD_A =~ 'a.*'))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeIOExceptionIterator();
        }
    }

    // IteratorTimeoutException on random operation
    @Test
    public void testFieldedRegexITEOnRandom() throws Exception {
        try {
            addIOExceptionIterator(IteratorTimeoutException.class.getName(), "ITE for test", "random");
            givenQuery("FIELD_A =~ 'a.*'");
            expectPlan("((_Value_ = true) && (FIELD_A =~ 'a.*'))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeIOExceptionIterator();
        }
    }

    // IterationInterruptedException on random operation
    @Test
    public void testFieldedRegexIIEOnRandom() throws Exception {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IIE for test", "random");
            givenQuery("FIELD_A =~ 'a.*'");
            expectPlan("((_Value_ = true) && (FIELD_A =~ 'a.*'))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testUnfieldedRegexTimeoutOnInitialSeek() {
        try {
            addDelayIterator(timeoutDelayMS);
            givenQuery("_ANYFIELD_ =~ 'a.*'");
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeDelayIterator();
        }
    }

    @Test
    public void testUnfieldedRegexTimeoutOnNext() {
        try {
            addDelayIterator();
            givenQuery("_ANYFIELD_ =~ 'a.*'");
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeDelayIterator();
        }
    }

    @Test
    public void testUnfieldedRegexNPEOnSeek() {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "seek");
            givenQuery("_ANYFIELD_ =~ 'a.*'");
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testUnfieldedRegexNPEOnNext() {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "next");
            givenQuery("_ANYFIELD_ =~ 'a.*'");
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testUnfieldedRegexNPERandom() {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "random");
            givenQuery("_ANYFIELD_ =~ 'a.*'");
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // IterationInterruptedException on seek
    @Test
    public void testUnfieldedRegexIIEOnInitialSeek() {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IIE for test", "seek");
            givenQuery("_ANYFIELD_ =~ 'a.*'");
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // IterationInterruptedException on next
    @Test
    public void testUnfieldedRegexIIEOnNext() {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IIE for test", "next");
            givenQuery("_ANYFIELD_ =~ 'a.*'");
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // IteratorTimeoutException on next
    @Test
    public void testUnfieldedRegexITEOnNext() {
        try {
            addIOExceptionIterator(IteratorTimeoutException.class.getName(), "ITE for test", "next");
            givenQuery("_ANYFIELD_ =~ 'a.*'");
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeIOExceptionIterator();
        }
    }

    // IteratorTimeoutException on random operation
    @Test
    public void testUnfieldedRegexITEOnRandom() {
        try {
            addIOExceptionIterator(IteratorTimeoutException.class.getName(), "ITE for test", "random");
            givenQuery("_ANYFIELD_ =~ 'a.*'");
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeIOExceptionIterator();
        }
    }

    // IterationInterruptedException on random operation
    @Test
    public void testUnfieldedRegexIIEOnRandom() {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IIE for test", "random");
            givenQuery("_ANYFIELD_ =~ 'a.*'");
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // This enters an infinite loop
    @Test
    @Disabled
    public void testBoundedRangeTimeoutOnInitialSeek() throws Exception {
        try {
            addDelayIterator(timeoutDelayMS);
            givenQuery("((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z'))");
            expectPlan("((_Value_ = true) && ((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z')))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeDelayIterator();
        }
    }

    @Test
    public void testBoundedRangeTimeoutOnNext() throws Exception {
        try {
            addDelayIterator();
            givenQuery("((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z'))");
            expectPlan("((_Value_ = true) && ((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z')))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeDelayIterator();
        }
    }

    @Test
    public void testBoundedRangeNPEOnSeek() {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "seek");
            givenQuery("((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z'))");
            // expectPlan("((_Value_ = true) && ((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z')))");
            // this is a perfectly valid query and should not be failing due to a NPE thrown all the up the stack
            assertThrows(NullPointerException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testBoundedRangeNPEOnNext() {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "next");
            givenQuery("((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z'))");
            // expectPlan("((_Value_ = true) && ((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z')))");
            // this is a perfectly valid query and should not be failing due to a NPE thrown all the up the stack
            assertThrows(NullPointerException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testBoundedRangeNPERandom() {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "random");
            givenQuery("((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z'))");
            expectPlan("((_Value_ = true) && ((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z')))");
            expectRegexExpansionTime(scanThresholdMS);
            // this is a perfectly valid query and should not be failing due to a NPE thrown all the up the stack
            assertThrows(NullPointerException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // IterationInterruptedException on seek
    @Test
    public void testBoundedRangeIIEOnInitialSeek() {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IIE for test", "seek");
            givenQuery("((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z'))");
            // expectPlan("((_Value_ = true) && ((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z')))");
            // this is throwing an IterationInterruptedException all the way up and failing a perfectly valid query
            assertThrows(IterationInterruptedException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // IterationInterruptedException on next
    @Test
    public void testBoundedRangeIIEOnNext() {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IIE for test", "next");
            givenQuery("((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z'))");
            // expectPlan("((_Value_ = true) && ((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z')))");
            // this is throwing an IterationInterruptedException all the way up and failing a perfectly valid query
            assertThrows(IterationInterruptedException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // IteratorTimeoutException on next
    @Test
    public void testBoundedRangeITEOnNext() throws Exception {
        try {
            addIOExceptionIterator(IteratorTimeoutException.class.getName(), "ITE for test", "next");
            givenQuery("((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z'))");
            expectPlan("((_Value_ = true) && ((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z')))");
            planAndExecuteQuery();
        } finally {
            removeIOExceptionIterator();
        }
    }

    // IteratorTimeoutException on random operation
    @Test
    public void testBoundedRangeITEOnRandom() throws Exception {
        try {
            addIOExceptionIterator(IteratorTimeoutException.class.getName(), "ITE for test", "random");
            givenQuery("((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z'))");
            expectPlan("((_Value_ = true) && ((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z')))");
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeIOExceptionIterator();
        }
    }

    // IterationInterruptedException on random operation
    @Test
    public void testBoundedRangeIIEOnRandom() {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IIE for test", "random");
            givenQuery("((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z'))");
            // expectPlan("((_Value_ = true) && ((_Bounded_ = true) && (FIELD_A >= 'a' && FIELD_A <= 'z')))");
            // this is throwing an IterationInterruptedException all the way up and failing a perfectly valid query
            assertThrows(IterationInterruptedException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testUnfieldedLiteralTimeoutOnInitialSeek() {
        try {
            addDelayIterator(timeoutDelayMS);
            givenQuery("_ANYFIELD_ == 'a1b2c3'");
            expectPlan("_NOFIELD_ == 'a1b2c3'");
            expectRegexExpansionTime(scanThresholdMS);
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeDelayIterator();
        }
    }

    @Test
    public void testUnfieldedLiteralTimeoutOnNext() {
        try {
            addDelayIterator();
            givenQuery("_ANYFIELD_ == 'a1b2c3'");
            expectPlan("_NOFIELD_ == 'a1b2c3'");
            expectRegexExpansionTime(scanThresholdMS);
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeDelayIterator();
        }
    }

    @Test
    public void testUnfieldedLiteralNPEOnSeek() {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "seek");
            givenQuery("_ANYFIELD_ == 'a1b2c3'");
            // This should be a DatawaveFatalException. We should not be passing up raw runtime exceptions.
            assertThrows(RuntimeException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testUnfieldedLiteralNPEOnNext() {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "next");
            givenQuery("_ANYFIELD_ == 'a1b2c3'");
            // This should be a DatawaveFatalException. We should not be passing up raw runtime exceptions.
            assertThrows(RuntimeException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testUnfieldedLiteralNPERandom() {
        try {
            addRuntimeExceptionIterator(NullPointerException.class.getName(), "NPE for test", "random");
            givenQuery("_ANYFIELD_ == 'a1b2c3'");
            // This should be a DatawaveFatalException. We should not be passing up raw runtime exceptions.
            assertThrows(RuntimeException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // IterationInterruptedException on seek
    @Test
    public void testUnfieldedLiteralIIEOnInitialSeek() {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IIE for test", "seek");
            givenQuery("_ANYFIELD_ == 'a1b2c3'");
            // This should be a DatawaveFatalException. We should not be passing up raw runtime exceptions.
            assertThrows(RuntimeException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // IterationInterruptedException on next
    @Test
    public void testUnfieldedLiteralIIEOnNext() {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IIE for test", "next");
            givenQuery("_ANYFIELD_ == 'a1b2c3'");
            // This should be a DatawaveFatalException. We should not be passing up raw runtime exceptions.
            assertThrows(RuntimeException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    // IteratorTimeoutException on next
    @Test
    public void testUnfieldedLiteralITEOnNext() {
        try {
            addIOExceptionIterator(IteratorTimeoutException.class.getName(), "ITE for test", "next");
            givenQuery("_ANYFIELD_ == 'a1b2c3'");
            // This should be a DatawaveFatalException. We should not be passing up raw runtime exceptions.
            assertThrows(RuntimeException.class, this::planAndExecuteQuery);
        } finally {
            removeIOExceptionIterator();
        }
    }

    // IteratorTimeoutException on random operation
    @Test
    public void testUnfieldedLiteralITEOnRandom() {
        try {
            addIOExceptionIterator(IteratorTimeoutException.class.getName(), "ITE for test", "random");
            givenQuery("_ANYFIELD_ == 'a1b2c3'");
            // This should be a DatawaveFatalException. We should not be passing up raw runtime exceptions.
            assertThrows(RuntimeException.class, this::planAndExecuteQuery);
        } finally {
            removeIOExceptionIterator();
        }
    }

    // IterationInterruptedException on random operation
    @Test
    public void testUnfieldedLiteralIIEOnRandom() {
        try {
            addRuntimeExceptionIterator(IterationInterruptedException.class.getName(), "IIE for test", "random");
            givenQuery("_ANYFIELD_ == 'a1b2c3'");
            // expectPlan("_NOFIELD_ == 'a1b2c3'");
            // This should be a DatawaveFatalException. We should not be passing up raw runtime exceptions.
            assertThrows(RuntimeException.class, this::planAndExecuteQuery);
        } finally {
            removeRuntimeExceptionIterator();
        }
    }

    @Test
    public void testMultiTermFieldedRegexTimeout() throws Exception {
        try {
            SortedSet<String> fields = new TreeSet<>(Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_E"));
            SortedSet<String> prefixes = new TreeSet<>(Set.of("aa", "ab", "ac", "ad", "ae"));

            String query = buildFieldedRegexQuery(fields, prefixes);
            String plan = buildValueExceededPlan(fields, prefixes);

            addIOExceptionIterator(IteratorTimeoutException.class.getName(), "ITE for test", "random");
            givenQuery(query);
            expectPlan(plan);
            expectRegexExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeIOExceptionIterator();
        }
    }

    @Test
    public void testMultiTermUnfieldedRegex() {
        try {
            SortedSet<String> prefixes = new TreeSet<>(Set.of("aa", "ab", "ac", "ad", "ae"));
            String query = buildUnfieldedRegexQuery(prefixes);

            addIOExceptionIterator(IteratorTimeoutException.class.getName(), "ITE for test", "random");
            givenQuery(query);
            expectPlan(null); // no plan expected
            assertThrows(DatawaveFatalQueryException.class, this::planAndExecuteQuery);
        } finally {
            removeIOExceptionIterator();
        }
    }

    @Test
    public void testMultiTermBoundedRange() throws Exception {
        try {
            SortedSet<String> fields = new TreeSet<>(Set.of("FIELD_A", "FIELD_B", "FIELD_C", "FIELD_D", "FIELD_E"));
            String query = buildBoundedRangeQuery(fields);
            String plan = buildValueExceededRangePlan(fields);

            addIOExceptionIterator(IteratorTimeoutException.class.getName(), "ITE for test", "random");
            givenQuery(query);
            expectPlan(plan);
            expectRangeExpansionTime(scanThresholdMS);
            planAndExecuteQuery();
        } finally {
            removeIOExceptionIterator();
        }
    }

    @Test
    public void testMultiTermUnfieldedLiteral() {

    }

    protected String buildFieldedRegexQuery(Set<String> fields, Set<String> values) {
        List<String> terms = new ArrayList<>();
        for (String field : fields) {
            for (String value : values) {
                terms.add(field + " =~ '" + value + ".*'");
            }
        }
        return Joiner.on(" || ").join(terms);
    }

    protected String buildUnfieldedRegexQuery(Set<String> values) {
        List<String> terms = new ArrayList<>();
        for (String value : values) {
            terms.add("_ANYFIELD_ =~ '" + value + ".*'");
        }
        return Joiner.on(" || ").join(terms);
    }

    protected String buildBoundedRangeQuery(Set<String> fields) {
        List<String> terms = new ArrayList<>();
        for (String field : fields) {
            terms.add("((_Bounded_ = true) && (" + field + " >= 'a' && " + field + " <= 'z'))");
        }
        return Joiner.on(" || ").join(terms);
    }

    protected String buildValueExceededPlan(Set<String> fields, Set<String> values) {
        List<String> terms = new ArrayList<>();
        for (String field : fields) {
            for (String value : values) {
                terms.add("((_Value_ = true) && (" + field + " =~ '" + value + ".*'))");
            }
        }
        return Joiner.on(" || ").join(terms);
    }

    protected String buildValueExceededRangePlan(Set<String> fields) {
        List<String> terms = new ArrayList<>();
        for (String field : fields) {
            terms.add("((_Value_ = true) && ((_Bounded_ = true) && (" + field + " >= 'a' && " + field + " <= 'z')))");
        }
        return Joiner.on(" || ").join(terms);
    }

    protected void addDelayIterator() {
        addDelayIterator(iterationDelayMS);
    }

    protected void addDelayIterator(int delay) {
        for (String tableName : indexTableNames()) {
            Map<String,String> properties = new HashMap<>();
            properties.put("table.iterator.scan.delay", "1,datawave.test.iter.DelayIterator");
            properties.put("table.iterator.scan.delay.opt.delay", String.valueOf(delay));
            MacTestUtil.addPropertiesAndWait(tops, tableName, properties);
        }
    }

    protected void removeDelayIterator() {
        for (String tableName : indexTableNames()) {
            Set<String> properties = new HashSet<>();
            properties.add("table.iterator.scan.delay");
            properties.add("table.iterator.scan.delay.opt.delay");
            MacTestUtil.removePropertiesAndWait(tops, tableName, properties);
        }
    }

    protected void addRuntimeExceptionIterator(String clazz, String msg, String when) {
        for (String tableName : indexTableNames()) {
            Map<String,String> properties = new HashMap<>();
            properties.put("table.iterator.scan.rex", "2,datawave.test.iter.RuntimeExceptionIterator");
            properties.put("table.iterator.scan.rex.opt.exception.class", String.valueOf(clazz));
            properties.put("table.iterator.scan.rex.opt.exception.message", String.valueOf(msg));
            switch (when) {
                case "seek":
                    properties.put("table.iterator.scan.rex.opt.fireOnSeek", "true");
                    break;
                case "next":
                    properties.put("table.iterator.scan.rex.opt.fireOnNext", "true");
                    break;
                case "random":
                    properties.put("table.iterator.scan.rex.opt.fireRandomly", "true");
                    break;
                default:
                    throw new IllegalStateException("unknown state: " + when);
            }
            MacTestUtil.addPropertiesAndWait(tops, tableName, properties);
        }
    }

    protected void removeRuntimeExceptionIterator() {
        for (String tableName : indexTableNames()) {
            Set<String> properties = new HashSet<>();
            properties.add("table.iterator.scan.rex");
            properties.add("table.iterator.scan.rex.opt.exception.class");
            properties.add("table.iterator.scan.rex.opt.exception.message");
            properties.add("table.iterator.scan.rex.opt.fireOnSeek");
            properties.add("table.iterator.scan.rex.opt.fireOnNext");
            properties.add("table.iterator.scan.rex.opt.fireRandomly");
            MacTestUtil.removePropertiesAndWait(tops, tableName, properties);
        }
    }

    protected void addIOExceptionIterator(String clazz, String msg, String when) {
        for (String tableName : indexTableNames()) {
            Map<String,String> properties = new HashMap<>();
            properties.put("table.iterator.scan.ioex", "3,datawave.test.iter.IOExceptionIterator");
            properties.put("table.iterator.scan.ioex.opt.exception.class", String.valueOf(clazz));
            properties.put("table.iterator.scan.ioex.opt.exception.message", String.valueOf(msg));
            switch (when) {
                case "seek":
                    properties.put("table.iterator.scan.ioex.opt.fireOnSeek", "true");
                    break;
                case "next":
                    properties.put("table.iterator.scan.ioex.opt.fireOnNext", "true");
                    break;
                case "random":
                    properties.put("table.iterator.scan.ioex.opt.fireRandomly", "true");
                    break;
                default:
                    throw new IllegalStateException("unknown state: " + when);
            }
            MacTestUtil.addPropertiesAndWait(tops, tableName, properties);
        }
    }

    protected void removeIOExceptionIterator() {
        for (String tableName : indexTableNames()) {
            Set<String> properties = new HashSet<>();
            properties.add("table.iterator.scan.ioex");
            properties.add("table.iterator.scan.ioex.opt.exception.class");
            properties.add("table.iterator.scan.ioex.opt.exception.message");
            properties.add("table.iterator.scan.ioex.opt.fireOnSeek");
            properties.add("table.iterator.scan.ioex.opt.fireOnNext");
            properties.add("table.iterator.scan.ioex.opt.fireRandomly");
            MacTestUtil.removePropertiesAndWait(tops, tableName, properties);
        }
    }

    /**
     * A simple stub that allows the test framework to grab the total elapsed time spend in certain stages of query planning.
     */
    private static class StageTimingDefaultQueryPlanner extends DefaultQueryPlanner {

        private long expandRegexStage = 0L;
        private long expandRangeStage = 0L;

        @Override
        protected ASTJexlScript timedExpandRegex(QueryStopwatch timers, String stage, final ASTJexlScript script, ShardQueryConfiguration config,
                        MetadataHelper metadataHelper, ScannerFactory scannerFactory, Map<String,IndexLookup> indexLookupMap) throws DatawaveQueryException {
            long start = System.currentTimeMillis();
            try {
                return super.timedExpandRegex(timers, stage, script, config, metadataHelper, scannerFactory, indexLookupMap);
            } finally {
                expandRegexStage = System.currentTimeMillis() - start;
                log.info("expand regexes: {}", expandRegexStage);
            }
        }

        @Override
        protected ASTJexlScript timedExpandRanges(QueryStopwatch timers, String stage, final ASTJexlScript script, ShardQueryConfiguration config,
                        MetadataHelper metadataHelper, ScannerFactory scannerFactory) throws DatawaveQueryException {
            long start = System.currentTimeMillis();
            try {
                return super.timedExpandRanges(timers, stage, script, config, metadataHelper, scannerFactory);
            } finally {
                expandRangeStage = System.currentTimeMillis() - start;
                log.info("expand ranges: {}", expandRangeStage);
            }
        }

        public long getExpandRegexStage() {
            return expandRegexStage;
        }

        public long getExpandRangeStage() {
            return expandRangeStage;
        }
    }
}
