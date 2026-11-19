package datawave.query;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterEach;
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

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcType;
import datawave.query.attributes.Document;
import datawave.query.exceptions.InvalidQueryException;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractIngest;
import datawave.query.util.AbstractQueryTest;
import datawave.table.constants.TableName;

/**
 * End-to-end tests for null and not-null checks against index-only fields, which are filtered at the field index when
 * {@link datawave.query.config.ShardQueryConfiguration#isAllowIndexOnlyNullChecks()} is enabled.
 * <p>
 * Data layout, all documents have FOO (indexed, in the event) and a UUID (in the event):
 * <ul>
 * <li>uid 1: FOO = value-a, no INDEX_ONLY</li>
 * <li>uid 2: FOO = value-a, INDEX_ONLY = zzzzzzz</li>
 * <li>uid 3: FOO = value-a, INDEX_ONLY = aaa and INDEX_ONLY = mmm</li>
 * <li>uid 4: FOO = value-other, INDEX_ONLY = zzzzzzz</li>
 * </ul>
 * INDEX_ONLY and INDEX_ONLY2 are index-only. INDEX_ONLY2 has no data at all.
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
public class IndexOnlyNullCheckIT extends AbstractQueryTest {

    @TempDir
    public static Path folder;

    private static final Authorizations auths = new Authorizations("ALL");

    protected static AccumuloClient client = null;

    protected static AbstractIngest ingest = null;

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
        // no-op
    }

    @Override
    protected void extraAssertions() {
        // no-op
    }

    @Override
    protected List<String> getIndexTableNames() {
        return List.of(TableName.SHARD_INDEX);
    }

    @BeforeAll
    public static void beforeAll() throws Exception {
        InMemoryInstance i = new InMemoryInstance(IndexOnlyNullCheckIT.class.getName());
        client = new InMemoryAccumuloClient("", i);

        ingest = new AbstractIngest(client, auths);
        ingest.registerField("UUID", new LcType());
        ingest.registerColumns("UUID", List.of("e"));

        ingest.registerField("FOO", new LcType());
        ingest.registerColumns("FOO", List.of("i", "e"));

        // index only: indexed, but never written to the event
        ingest.registerField("INDEX_ONLY", new LcType());
        ingest.registerColumns("INDEX_ONLY", List.of("i"));

        // index only, and no data
        ingest.registerField("INDEX_ONLY2", new LcType());
        ingest.registerColumns("INDEX_ONLY2", List.of("i"));

        ingest.writeFV(1, "UUID", "uid-1");
        ingest.writeFV(1, "FOO", "value-a");

        ingest.writeFV(2, "UUID", "uid-2");
        ingest.writeFV(2, "FOO", "value-a");
        ingest.writeFV(2, "INDEX_ONLY", "zzzzzzz");

        ingest.writeFV(3, "UUID", "uid-3");
        ingest.writeFV(3, "FOO", "value-a");
        ingest.writeFV(3, "INDEX_ONLY", "aaa");
        ingest.writeFV(3, "INDEX_ONLY", "mmm");

        ingest.writeFV(4, "UUID", "uid-4");
        ingest.writeFV(4, "FOO", "value-other");
        ingest.writeFV(4, "INDEX_ONLY", "zzzzzzz");
    }

    @BeforeEach
    public void beforeEach() {
        setClientForTest(client);
    }

    @AfterEach
    public void resetFlag() {
        logic.getConfig().setAllowIndexOnlyNullChecks(false);
    }

    private void givenFlag(boolean enabled) {
        logic.getConfig().setAllowIndexOnlyNullChecks(enabled);
    }

    @Test
    public void printTables() {
        ingest.printTable(TableName.METADATA);
        ingest.printTable(TableName.SHARD);
    }

    /**
     * Verify no issues with a single anchor term.
     */
    @Test
    public void testAnchorOnly() throws Exception {
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a'");
        expectPlan("FOO == 'value-a'");
        expectUUIDs(Set.of("uid-1", "uid-2", "uid-3"));
        planAndExecuteQuery();
    }

    /**
     * Verify we can execute {@code INDEX_ONLY == null}.
     */
    @Test
    public void testIsNull() throws Exception {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' && INDEX_ONLY == null");
        expectPlan("FOO == 'value-a' && INDEX_ONLY == null");
        expectUUIDs(Set.of("uid-1"));
        planAndExecuteQuery();
    }

    @Test
    public void testNegatedIsNull() throws Exception {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' && !(INDEX_ONLY == null)");
        expectPlan("FOO == 'value-a' && !(INDEX_ONLY == null)");
        expectUUIDs(Set.of("uid-2", "uid-3"));
        planAndExecuteQuery();
        assertIndexOnlyFieldsNotReturned();
    }

    @Test
    public void testIsNotNull() throws Exception {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' && INDEX_ONLY != null");
        // the planner rewrites FIELD != null as !(FIELD == null)
        expectPlan("FOO == 'value-a' && !(INDEX_ONLY == null)");
        expectUUIDs(Set.of("uid-2", "uid-3"));
        planAndExecuteQuery();
        assertIndexOnlyFieldsNotReturned();
    }

    @Test
    public void testIsNullAbsentField() throws Exception {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' && INDEX_ONLY2 == null");
        expectPlan("FOO == 'value-a' && INDEX_ONLY2 == null");
        expectUUIDs(Set.of("uid-1", "uid-2", "uid-3"));
        planAndExecuteQuery();
    }

    @Test
    public void testIsNotNullAbsentField() throws Exception {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' && INDEX_ONLY2 != null");
        // the planner rewrites FIELD != null as !(FIELD == null)
        expectPlan("FOO == 'value-a' && !(INDEX_ONLY2 == null)");
        expectResultCount(0);
        planAndExecuteQuery();
    }

    /**
     * A null and a not-null check against different index-only fields. uid-3 has two values for INDEX_ONLY and must be returned once.
     */
    @Test
    public void testIsNotNullAndIsNull() throws Exception {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' && INDEX_ONLY != null && INDEX_ONLY2 == null");
        expectPlan("FOO == 'value-a' && !(INDEX_ONLY == null) && INDEX_ONLY2 == null");
        expectResultCount(2);
        expectUUIDs(Set.of("uid-2", "uid-3"));
        planAndExecuteQuery();
        assertIndexOnlyFieldsNotReturned();
    }

    /**
     * An OR of null checks ANDed with an anchor. Both branches match, so every anchored document is returned.
     */
    @Test
    public void testOrOfIsNullAndIsNotNull() throws Exception {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' && (INDEX_ONLY == null || INDEX_ONLY != null)");
        expectPlan("FOO == 'value-a' && (INDEX_ONLY == null || !(INDEX_ONLY == null))");
        expectResultCount(3);
        expectUUIDs(Set.of("uid-1", "uid-2", "uid-3"));
        planAndExecuteQuery();
        assertIndexOnlyFieldsNotReturned();
    }

    /**
     * An OR of null checks ANDed with an anchor. INDEX_ONLY2 has no data, so only the INDEX_ONLY branch matches.
     */
    @Test
    public void testOrOfIsNullAndIsNotNullAbsentField() throws Exception {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' && (INDEX_ONLY == null || INDEX_ONLY2 != null)");
        expectPlan("FOO == 'value-a' && (INDEX_ONLY == null || !(INDEX_ONLY2 == null))");
        expectUUIDs(Set.of("uid-1"));
        planAndExecuteQuery();
    }

    @Test
    public void testOrOfIsNullFlagDisabled() {
        givenFlag(false);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' && (INDEX_ONLY == null || INDEX_ONLY2 != null)");
        disableQueryPlanAssertion();
        assertThrows(InvalidQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    public void testIsNullFlagDisabled() {
        givenFlag(false);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' && INDEX_ONLY == null");
        disableQueryPlanAssertion();
        assertThrows(InvalidQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    public void testIsNotNullFlagDisabled() {
        givenFlag(false);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' && INDEX_ONLY != null");
        disableQueryPlanAssertion();
        assertThrows(InvalidQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    public void testIsNullWithoutAnchorFlagEnabled() {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("INDEX_ONLY == null");
        disableQueryPlanAssertion();
        assertThrows(InvalidQueryException.class, this::planAndExecuteQuery);
    }

    /**
     * An index-only null check must be ANDed with an executable term. ORed with one, it has no anchor.
     */
    @Test
    public void testIsNullOrAnchorFlagEnabled() {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' || INDEX_ONLY == null");
        disableQueryPlanAssertion();
        assertThrows(InvalidQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    public void testIsNotNullOrAnchorFlagEnabled() {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' || INDEX_ONLY != null");
        disableQueryPlanAssertion();
        assertThrows(InvalidQueryException.class, this::planAndExecuteQuery);
    }

    @Test
    public void testNegatedIsNullOrAnchorFlagEnabled() {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' || !(INDEX_ONLY == null)");
        disableQueryPlanAssertion();
        assertThrows(InvalidQueryException.class, this::planAndExecuteQuery);
    }

    /**
     * The executable term is ORed with the null check, so it does not anchor it. The other AND sibling is also a null check, so there is no anchor.
     */
    @Test
    public void testIsNullOrAnchorAndIsNullFlagEnabled() {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("(FOO == 'value-a' || INDEX_ONLY == null) && INDEX_ONLY2 == null");
        disableQueryPlanAssertion();
        assertThrows(InvalidQueryException.class, this::planAndExecuteQuery);
    }

    /**
     * The null check is ORed with an executable term, but the OR is ANDed with an anchor, so the query is valid. No anchored document has FOO = value-other, so
     * only the null check can match.
     */
    @Test
    public void testIsNullOrExecutableWithAnchor() throws Exception {
        givenFlag(true);
        givenDate(ingest.getDate());
        givenQuery("FOO == 'value-a' && (FOO == 'value-other' || INDEX_ONLY == null)");
        expectPlan("FOO == 'value-a' && (FOO == 'value-other' || INDEX_ONLY == null)");
        expectUUIDs(Set.of("uid-1"));
        planAndExecuteQuery();
    }

    /**
     * The presence of index-only fields is used for evaluation only, it should not be returned.
     */
    private void assertIndexOnlyFieldsNotReturned() {
        for (Document result : results) {
            assertNull(result.get("INDEX_ONLY"), "result should not contain INDEX_ONLY" + result);
            assertNull(result.get("INDEX_ONLY2"), "result should not contain INDEX_ONLY2" + result);
        }
    }

}
