package datawave.query;

import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.ingest.data.TypeRegistry;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.TestIndexTableNames;
import datawave.query.util.WiseGuysIngest;
import datawave.table.constants.TableName;

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
public class IvaratorInterruptTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(IvaratorInterruptTest.class);
    private static final Authorizations auths = new Authorizations("ALL");

    @TempDir
    public static Path metadataDir;

    private static AccumuloClient client;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    @BeforeAll
    public static void beforeAll() throws Exception {
        System.setProperty("type.metadata.dir", metadataDir.toFile().getCanonicalPath());

        QueryTestTableHelper qtth = new QueryTestTableHelper(IvaratorInterruptTest.class.toString(), log, RebuildingScannerTestHelper.TEARDOWN.NEVER,
                        RebuildingScannerTestHelper.INTERRUPT.FI_EVERY_OTHER);
        client = qtth.client;

        WiseGuysIngest.writeItAll(client, WiseGuysIngest.WhatKindaRange.DOCUMENT);

        IndexIngestUtil util = new IndexIngestUtil();
        util.write(client, auths);
    }

    @AfterAll
    public static void afterAll() {
        TypeRegistry.reset();
    }

    @BeforeEach
    public void setup(@TempDir Path ivaratorCacheDir) throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
        setClientForTest(client);

        logic.setFullTableScanEnabled(true);
        // this should force regex expansion into ivarators
        logic.setMaxValueExpansionThreshold(1);

        // setup the hadoop configuration
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());

        // setup a directory for cache results
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(ivaratorCacheDir.toUri().toString());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(config));
    }

    @Override
    public ShardQueryLogic getLogic() {
        return logic;
    }

    @Override
    public Authorizations getAuths() {
        return auths;
    }

    @Override
    protected void extraConfigurations() {
        disableQueryPlanAssertion();
    }

    @Override
    protected void extraAssertions() {
        // no-op
    }

    /**
     * This test is about exercising ivarator interrupts during shard scans, so only run against the index table types that force a full/shard scan.
     *
     * @return a subset of the standard index tables
     */
    @Override
    protected List<String> getIndexTableNames() {
        return List.of(TableName.SHARD_INDEX, TestIndexTableNames.NO_UID_INDEX);
    }

    @Test
    public void testIvaratorInterruptedUnsorted() throws Exception {
        givenDate("20121231", "20130102");
        givenQuery("UUID =~ '^[CS].*'");
        expectUUIDs(Set.of("CORLEONE", "SOPRANO", "CAPONE"));

        planAndExecuteQuery();
    }

    @Test
    public void testIvaratorInterruptedSorted() throws Exception {
        Map<String,String> params = new HashMap<>();
        // both required in order to force ivarator to call fillSets
        params.put(QueryOptions.SORTED_UIDS, "true");
        logic.getConfig().setUnsortedUIDsEnabled(false);

        givenDate("20121231", "20130102");
        givenQuery("UUID =~ '^[CS].*'");
        givenParameters(params);
        expectUUIDs(Set.of("CORLEONE", "SOPRANO", "CAPONE"));

        planAndExecuteQuery();
    }
}
