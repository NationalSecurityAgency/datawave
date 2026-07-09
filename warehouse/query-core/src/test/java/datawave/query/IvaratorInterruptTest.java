package datawave.query;

import java.io.IOException;
import java.net.URL;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.query.iterator.QueryOptions;
import datawave.query.iterator.ivarator.IvaratorCacheDirConfig;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.WiseGuysIngest;
import datawave.query.util.WiseGuysIngest.WhatKindaRange;

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

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

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

    private void init(WhatKindaRange range, Path tempDir) throws Exception {
        System.setProperty("type.metadata.dir", tempDir.toFile().getCanonicalPath() + "/" + range.name());

        QueryTestTableHelper qtth = new QueryTestTableHelper(IvaratorInterruptTest.class.toString() + "-" + range, log,
                        RebuildingScannerTestHelper.TEARDOWN.NEVER, RebuildingScannerTestHelper.INTERRUPT.FI_EVERY_OTHER);
        AccumuloClient client = qtth.client;
        WiseGuysIngest.writeItAll(client, range);
        setClientForTest(client);
    }

    private void configureLogic(WhatKindaRange range, Path tempDir) throws IOException {
        logic.setFullTableScanEnabled(true);
        // this should force regex expansion into ivarators
        logic.setMaxValueExpansionThreshold(1);

        // setup the hadoop configuration
        URL hadoopConfig = this.getClass().getResource("/testhadoop.config");
        logic.setHdfsSiteConfigURLs(hadoopConfig.toExternalForm());

        // setup a directory for cache results
        IvaratorCacheDirConfig config = new IvaratorCacheDirConfig(tempDir.toUri().toString());
        logic.setIvaratorCacheDirConfigs(Collections.singletonList(config));

        logic.setCollapseUids(range == WhatKindaRange.SHARD);
        logic.setMaxEvaluationPipelines(1);
    }

    @ParameterizedTest
    @EnumSource(WhatKindaRange.class)
    public void testIvaratorInterruptedUnsorted(WhatKindaRange range, @TempDir Path tempDir) throws Exception {
        init(range, tempDir);
        configureLogic(range, tempDir);

        givenDate("20091231", "20150101");
        givenQuery("UUID =~ '^[CS].*'");
        expectUUIDs(Set.of("CORLEONE", "SOPRANO", "CAPONE"));

        planAndExecuteQuery();
    }

    @ParameterizedTest
    @EnumSource(WhatKindaRange.class)
    public void testIvaratorInterruptedSorted(WhatKindaRange range, @TempDir Path tempDir) throws Exception {
        init(range, tempDir);
        configureLogic(range, tempDir);

        Map<String,String> params = new HashMap<>();
        // both required in order to force ivarator to call fillSets
        params.put(QueryOptions.SORTED_UIDS, "true");
        logic.getConfig().setUnsortedUIDsEnabled(false);

        givenDate("20091231", "20150101");
        givenQuery("UUID =~ '^[CS].*'");
        givenParameters(params);
        expectUUIDs(Set.of("CORLEONE", "SOPRANO", "CAPONE"));

        planAndExecuteQuery();
    }
}
