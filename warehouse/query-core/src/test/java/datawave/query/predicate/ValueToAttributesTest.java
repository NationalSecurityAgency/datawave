package datawave.query.predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.helpers.PrintUtility;
import datawave.ingest.data.TypeRegistry;
import datawave.marking.AccessExpressionMarkings;
import datawave.marking.MarkingFunctions;
import datawave.query.QueryTestTableHelper;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.PreNormalizedAttribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.composite.CompositeMetadata;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractQueryTest;
import datawave.query.util.CompositeTestingIngest;
import datawave.query.util.TypeMetadata;
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
public class ValueToAttributesTest extends AbstractQueryTest {

    private static final Logger log = Logger.getLogger(ValueToAttributesTest.class);
    private static final Authorizations auths = new Authorizations("ALL");

    private static AccumuloClient clientForTest;

    @Autowired
    @Qualifier("EventQuery")
    protected ShardQueryLogic logic;

    private List<String> expectedList;

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
        String plannedScript = logic.getQueryPlanner().getPlannedScript();
        assertTrue(plannedScript.contains("MAKE_COLOR"), "CompositeTerm was not substituted into query:" + plannedScript);

        HashSet<String> resultSet = new HashSet<>();
        for (Document d : results) {
            Attribute<?> attr = d.get("UUID");
            if (attr == null)
                attr = d.get("UUID.0");

            assertNotNull(attr, "Result Document did not contain a 'UUID'");
            assertTrue(attr instanceof TypeAttribute || attr instanceof PreNormalizedAttribute,
                            "Expected result to be an instance of DatwawaveTypeAttribute, was: " + attr.getClass().getName());

            TypeAttribute<?> uuidAttr = (TypeAttribute<?>) attr;

            String uuid = uuidAttr.getType().getDelegate().toString();
            assertTrue(expectedList.contains(uuid), "Received unexpected UUID: " + uuid);

            resultSet.add(uuid);
        }

        assertTrue(expectedList.containsAll(resultSet), "Expected results " + expectedList + " differ form actual results " + resultSet);
        assertEquals(expectedList.size(), resultSet.size(), "Unexpected number of records");
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        TimeZone.setDefault(TimeZone.getTimeZone("GMT"));

        QueryTestTableHelper qtth = new QueryTestTableHelper(ValueToAttributesTest.class.toString(), log);
        clientForTest = qtth.client;

        // ingest with the document range only; CompositeTestingIngest already uses IndexIngestUtil
        // internally to derive the other shard index table variants that AbstractQueryTest iterates over.
        CompositeTestingIngest.writeItAll(clientForTest, CompositeTestingIngest.WhatKindaRange.DOCUMENT);
        PrintUtility.printTable(clientForTest, auths, TableName.SHARD);
        PrintUtility.printTable(clientForTest, auths, TableName.SHARD_INDEX);
        PrintUtility.printTable(clientForTest, auths, TableName.METADATA);
    }

    @AfterAll
    public static void afterAll() {
        TypeRegistry.reset();
    }

    @BeforeEach
    public void setup() {
        setClientForTest(clientForTest);
        logic.setCollapseUids(false);

        givenDate("20091231", "20150101");
    }

    private void runTestQuery(List<String> expected, String querystr, Map<String,String> extraParms) throws Exception {
        this.expectedList = expected;
        givenQuery(querystr);
        givenParameters(extraParms);

        planAndExecuteQuery();
    }

    @Test
    public void testCompositeFunctions() throws Exception {
        Map<String,String> extraParameters = new HashMap<>();
        extraParameters.put("include.grouping.context", "true");
        extraParameters.put("hit.list", "true");

        if (log.isDebugEnabled()) {
            log.debug("testCompositeFunctions");
        }
        String[] queryStrings = { //
                "COLOR == 'RED' && MAKE == 'FORD'", //
                "COLOR == 'BLUE' && MAKE == 'CHEVY'", //
                "COLOR == 'BLUE' && MAKE == 'FORD'", //
        };

        @SuppressWarnings("unchecked")
        List<String>[] expectedLists = new List[] { //
                Arrays.asList("One"), //
                Arrays.asList("One"), //
                Arrays.asList("One")//
        };
        for (int i = 0; i < queryStrings.length; i++) {
            runTestQuery(expectedLists[i], queryStrings[i], extraParameters);
        }
    }

    @Test
    public void testComposites() {
        CompositeMetadata compositeMetadata = new CompositeMetadata();
        for (String ingestType : new String[] {"test", "pilot", "work", "beep", "tw"}) {
            compositeMetadata.setCompositeFieldMappingByType(ingestType, "MAKE_COLOR", Arrays.asList("MAKE", "COLOR"));
            compositeMetadata.setCompositeFieldMappingByType(ingestType, "COLOR_WHEELS", Arrays.asList("MAKE", "COLOR"));
        }
        TypeMetadata typeMetadata = new TypeMetadata(
                        "dts:[0:beep];types:[0:datawave.data.type.DateType,1:datawave.data.type.IpAddressType,2:datawave.data.type.LcNoDiacriticsType,3:datawave.data.type.NoOpType,4:datawave.data.type.NumberType];MAKE:[0:2];MAKE_COLOR:[0:3];START_DATE:[0:0];TYPE_NOEVAL:[0:2];IP_ADDR:[0:1];WHEELS:[0:2,0:4];COLOR:[0:2];COLOR_WHEELS:[0:3];TYPE:[0:2]");

        MarkingFunctions<AccessExpressionMarkings> markingFunctions = new MarkingFunctions.Default();
        ValueToAttributes valueToAttributes = new ValueToAttributes(compositeMetadata, typeMetadata, null, markingFunctions, true);
    }
}
