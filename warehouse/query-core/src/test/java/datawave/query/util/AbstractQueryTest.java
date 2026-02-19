package datawave.query.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.jupiter.api.AfterEach;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.index.day.IndexIngestUtil;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.tables.ShardQueryLogic;
import datawave.test.HitTermAssertions;

/**
 * Class that holds common methods useful for integration testing queries.
 * <p>
 * This test framework is compatible with either an {@link InMemoryInstance} or {@link MiniAccumuloCluster}. It only requires an {@link AccumuloClient}.
 * <p>
 * Helper methods such as {@link #givenDate(String)} or {@link #givenQuery(String)} denote initial state while methods such as {@link #expectedResultCount} or
 * {@link #expectHitTermsRequiredAllOf(String...)} denote expected final state.
 * <p>
 * Initial state methods include:
 * <ul>
 * <li>{@link #givenDate(String)} required</li>
 * <li>{@link #givenDate(String, String)} required</li>
 * <li>{@link #givenQuery(String)} required</li>
 * <li>{@link #givenParameter(String, String)} optional</li>
 * <li>{@link #givenParameters(Map)} optional</li>
 * </ul>
 * <p>
 * Final state methods include:
 * <ul>
 * <li>{@link #expectPlan(String)} required</li>
 * <li>{@link #expectResultCount(int)} optional</li>
 * <li>{@link #expectHitTermsRequiredAnyOf(String...)}</li>
 * <li>{@link #expectHitTermsRequiredAllOf(String...)}</li>
 * <li>{@link #expectHitTermsOptionalAnyOf(String...)}</li>
 * <li>{@link #expectHitTermsOptionalAllOf(String...)}</li>
 * </ul>
 * Unit tests that extend this class should adhere to the method order presented above for the sake of consistency.
 * <p>
 * Additionally if a test is going to use {@link #givenParameter(String, String)} repeatedly for a one or more parameters it may be helped to wrap the method
 * call like <code>withDatatypeFilter</code>
 * <p>
 * This utility also provides native support for iteratively running the same query against multiple versions of the shard index table, as defined by
 * {@link TestIndexTableNames#names()}. Extending classes must use the {@link IndexIngestUtil} to populate the extra tables with data.
 */
public abstract class AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractQueryTest.class);

    protected final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    protected final KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();

    private AccumuloClient clientForTest;

    // initial state
    private String startDate;
    private String endDate;
    private String query;
    private final Map<String,String> parameters = new HashMap<>();

    // final state
    private String expectedQueryPlan = null;
    private int expectedResultCount = -1;
    protected final Set<Document> results = new HashSet<>();
    protected final Set<String> expectedUUIDs = new HashSet<>();
    private final HitTermAssertions hitTermAssertions = new HitTermAssertions();

    // the test framework is opinionated about asserting the final query plan
    private boolean queryPlanAssertionEnabled = true;

    public abstract ShardQueryLogic getLogic();

    /**
     * Authorizations may be different depending on the test
     *
     * @return some auths
     */
    public abstract Authorizations getAuths();

    @AfterEach
    public void afterEach() {
        // reset initial state
        startDate = null;
        endDate = null;
        query = null;
        parameters.clear();

        // reset expectations
        expectedQueryPlan = null;
        expectedResultCount = -1;
        results.clear();
        expectedUUIDs.clear();
        hitTermAssertions.resetState();

        queryPlanAssertionEnabled = true;
    }

    /**
     * Just because you <b>can</b> disable this doesn't mean you <b>should</b>.
     * <p>
     * Encoding the final query plan in a unit test conveys information that would otherwise be invisible.
     */
    protected void disableQueryPlanAssertion() {
        queryPlanAssertionEnabled = false;
    }

    public void setClientForTest(AccumuloClient client) {
        this.clientForTest = client;
    }

    protected String getQuery() {
        Preconditions.checkNotNull(query, "query is null");
        return query;
    }

    protected Date getStartDate() throws Exception {
        assertNotNull(startDate);
        return format.parse(startDate);
    }

    protected Date getEndDate() throws Exception {
        assertNotNull(endDate);
        return format.parse(endDate);
    }

    /**
     * Set the query start and end date
     *
     * @param date
     *            the date
     */
    public void givenDate(String date) {
        givenDate(date, date);
    }

    /**
     * Set the query start and end date
     *
     * @param startDate
     *            the start date
     * @param endDate
     *            the end date
     */
    public void givenDate(String startDate, String endDate) {
        this.startDate = startDate;
        this.endDate = endDate;
    }

    /**
     * Set the query string
     *
     * @param query
     *            the query string
     */
    public void givenQuery(String query) {
        this.query = query;
    }

    /**
     * Add the provided key-value pair to the query parameter map
     *
     * @param key
     *            the key
     * @param value
     *            the value
     */
    public void givenParameter(String key, String value) {
        parameters.put(key, value);
    }

    /**
     * Add the provided key-value pairs to the query parameter map
     *
     * @param parameters
     *            the map of parameters to set
     */
    public void givenParameters(Map<String,String> parameters) {
        this.parameters.putAll(parameters);
    }

    /**
     * Set the expected query plan
     *
     * @param queryPlan
     *            the expected query plan
     */
    public void expectPlan(String queryPlan) {
        this.expectedQueryPlan = queryPlan;
    }

    /**
     * Set the expected number of results
     *
     * @param expectedResultCount
     *            the expected number of results
     */
    public void expectResultCount(int expectedResultCount) {
        this.expectedResultCount = expectedResultCount;
    }

    /**
     * Set the expected UUIDs. Optional as not every test will have a 'UUID' field.
     *
     * @param expectedUUIDs
     *            the expected uuids
     */
    public void expectUUIDs(Set<String> expectedUUIDs) {
        this.expectedUUIDs.addAll(expectedUUIDs);
    }

    /**
     * Add a collection of hit terms to the set of required hit terms, evaluated as 'all of'
     *
     * @param hitTerms
     *            a collection of hit terms
     */
    public void expectHitTermsRequiredAllOf(String... hitTerms) {
        hitTermAssertions.withRequiredAllOf(hitTerms);
    }

    /**
     * Add a collection of hit terms to the set of required hit terms, evaluated as 'any of'
     *
     * @param hitTerms
     *            a collection of hit terms
     */
    public void expectHitTermsRequiredAnyOf(String... hitTerms) {
        hitTermAssertions.withRequiredAnyOf(hitTerms);
    }

    /**
     * Add a collection of hits to the list of optional hit terms, evaluated as 'all of'
     *
     * @param hitTerms
     *            a collection of hit terms
     */
    public void expectHitTermsOptionalAllOf(String... hitTerms) {
        hitTermAssertions.withOptionalAllOf(hitTerms);
    }

    /**
     * Add a collection of hits to the list of optional hit terms, evaluated as 'any of'
     *
     * @param hitTerms
     *            a collection of hit terms
     */
    public void expectHitTermsOptionalAnyOf(String... hitTerms) {
        hitTermAssertions.withOptionalAnyOf(hitTerms);
    }

    /**
     * A core method that will execute the query against every supported type of index table, as defined in {@link TestIndexTableNames#names()}.
     *
     * @throws Exception
     *             if something goes wrong
     */
    public void planAndExecuteQuery() throws Exception {
        planAndExecuteQuery(getLogic());
    }

    /**
     * A core method that will execute the query against every supported type of index table, as defined in {@link TestIndexTableNames#names()}.
     *
     * @param logic
     *            the query logic
     * @throws Exception
     *             if something goes wrong
     */
    public void planAndExecuteQuery(ShardQueryLogic logic) throws Exception {
        for (String indexTableName : TestIndexTableNames.names()) {
            try {
                log.info("=== using index: {} ===", indexTableName);
                setupIndexTable(indexTableName, logic);
                planAndExecuteIndividualQuery(logic);
            } finally {
                teardownIndexTable(indexTableName, logic);
            }
        }
    }

    /**
     * Configure the logic based on the index table name
     *
     * @param indexTableName
     *            the global index table name
     * @param logic
     *            the query logic
     */
    private void setupIndexTable(String indexTableName, ShardQueryLogic logic) {
        logic.setIndexTableName(indexTableName);
        switch (indexTableName) {
            case TestIndexTableNames.SHARD_INDEX:
            case TestIndexTableNames.NO_UID_INDEX:
                break;
            case TestIndexTableNames.TRUNCATED_INDEX:
                logic.setUseTruncatedIndex(true);
                break;
            default:
                throw new IllegalStateException("Unknown index table name: " + indexTableName);
        }
    }

    /**
     * Cleanup the logic based on the index table name
     *
     * @param indexTableName
     *            the global index table name
     * @param logic
     *            the query logic
     */
    private void teardownIndexTable(String indexTableName, ShardQueryLogic logic) {
        switch (indexTableName) {
            case TestIndexTableNames.SHARD_INDEX:
            case TestIndexTableNames.NO_UID_INDEX:
                break;
            case TestIndexTableNames.TRUNCATED_INDEX:
                getLogic().setUseTruncatedIndex(false);
                break;
            default:
                throw new IllegalStateException("Unknown index table name: " + indexTableName);
        }
    }

    /**
     * A core method that plans and executes the query
     *
     * @param logic
     *            the query logic
     * @throws Exception
     *             if something goes wrong
     */
    private void planAndExecuteIndividualQuery(ShardQueryLogic logic) throws Exception {
        planQuery(logic);
        executeQuery(logic);
        assertQueryPlan(logic);
        assertResultCount();
        assertUuids();
        assertHitTerms();
        extraAssertions();
    }

    /**
     * Configure the logic with start and end date, query parameters and plan the query.
     *
     * @param logic
     *            the query logic
     * @throws Exception
     *             if something goes wrong
     */
    private void planQuery(ShardQueryLogic logic) throws Exception {
        try {
            QueryImpl settings = new QueryImpl();
            settings.setBeginDate(getStartDate());
            settings.setEndDate(getEndDate());
            settings.setPagesize(Integer.MAX_VALUE);
            settings.setQueryAuthorizations(getAuths().serialize());
            settings.setQuery(getQuery());
            settings.setParameters(parameters);
            settings.setId(UUID.randomUUID());

            logic.setMaxEvaluationPipelines(1);
            logic.setHitList(true); // always ask for HIT_TERMs

            extraConfigurations();

            GenericQueryConfiguration config = logic.initialize(clientForTest, settings, Collections.singleton(getAuths()));
            logic.setupQuery(config);
        } catch (Exception e) {
            log.info("exception while planning query", e);
            throw e;
        }
    }

    /**
     * Extending classes may wish to perform additional 'just in time' configuration
     */
    protected abstract void extraConfigurations();

    /**
     * Iterate through the query and add all deserialized results to the set of result documents.
     *
     * @param logic
     *            the query logic
     */
    private void executeQuery(ShardQueryLogic logic) {
        results.clear();
        for (Map.Entry<Key,Value> entry : logic) {
            Document d = deserializer.apply(entry).getValue();
            results.add(d);
        }
        logic.close();
        log.info("query retrieved {} results", results.size());
    }

    /**
     * Assert the final query plan against the expected query plan. This assertion is <b>highly recommended</b> for every test.
     * <p>
     * Even though this assertion can be skipped via {@link #disableQueryPlanAssertion()}, if the user sets an expected query plan then the disable flag will be
     * ignored.
     *
     * @param logic
     *            the query logic
     */
    private void assertQueryPlan(ShardQueryLogic logic) {
        if (expectedQueryPlan == null && !queryPlanAssertionEnabled) {
            // only skip if the plan is null and plan assertions are disabled.
            // if plan assertions are disabled but an expected plan was set we must check the plan
            return;
        }

        assertNotNull(expectedQueryPlan, "The expected query plan must be set ");
        try {
            ASTJexlScript expected = JexlASTHelper.parseAndFlattenJexlQuery(expectedQueryPlan);
            ASTJexlScript plannedScript = logic.getConfig().getQueryTree();
            if (!TreeEqualityVisitor.isEqual(expected, plannedScript)) {
                log.info("expected: {}", expectedQueryPlan);
                log.info("planned : {}", logic.getConfig().getQueryString());
                fail("Planned query did not match expectation: " + TreeEqualityVisitor.checkEquality(expected, plannedScript).getReason());
            }
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        }
    }

    /**
     * An optional assertion for the total result count
     */
    private void assertResultCount() {
        if (expectedResultCount != -1) {
            assertEquals(expectedResultCount, results.size());
        }
    }

    /**
     * An optional assertion for the UUID field
     */
    public void assertUuids() {
        if (expectedUUIDs.isEmpty()) {
            return;
        }

        assertFalse(results.isEmpty(), "UUIDs are expected but results are empty");

        Set<String> found = new HashSet<>();
        for (Document result : results) {
            Attribute<?> attr = result.get("UUID");
            if (attr == null) {
                // this should be a little more dynamic
                attr = result.get("UUID.0");
            }
            assertNotNull(attr, "result did not contain a UUID");
            String uuid = getUUID(attr);
            found.add(uuid);
        }

        Set<String> missing = Sets.difference(expectedUUIDs, found);
        if (!missing.isEmpty()) {
            log.info("missing uuids: {}", missing);
        }

        Set<String> extra = Sets.difference(found, expectedUUIDs);
        if (!extra.isEmpty()) {
            log.info("extra uuids: {}", extra);
        }

        assertEquals(expectedUUIDs, new TreeSet<>(found));
    }

    public String getUUID(Attribute<?> attribute) {
        boolean typed = attribute instanceof TypeAttribute;
        assertTrue(typed, "Attribute was not a TypeAttribute, was: " + attribute.getClass());
        TypeAttribute<?> uuid = (TypeAttribute<?>) attribute;
        return uuid.getType().getDelegateAsString();
    }

    /**
     * An optional assertion for hit terms
     */
    private void assertHitTerms() {
        if (hitTermAssertions.hitTermExpected()) {
            assertEquals(hitTermAssertions.hitTermExpected(), !results.isEmpty());
            if (!results.isEmpty()) {
                boolean validated = hitTermAssertions.assertHitTerms(results);
                assertEquals(hitTermAssertions.hitTermExpected(), validated);
            }
        }
    }

    /**
     * Extending classes may insert additional assertions here.
     */
    protected abstract void extraAssertions();
}
