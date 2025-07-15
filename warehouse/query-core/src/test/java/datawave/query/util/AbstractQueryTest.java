package datawave.query.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ParseException;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.QueryImpl;
import datawave.query.attributes.Document;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.TreeEqualityVisitor;
import datawave.query.tables.ShardQueryLogic;
import datawave.test.HitTermAssertions;

/**
 * Class that holds common methods useful for integration testing queries.
 * <p>
 * This test framework is compatible with either an {@link InMemoryInstance} or {@link MiniAccumuloCluster}. It only requires an {@link AccumuloClient}.
 * <p>
 * Features include
 * <ul>
 * <li>Defining lucene or jexl queries</li>
 * <li>Defining query parameters</li>
 * <li>Defining expected hit terms</li>
 * <li>Defining expected result shards</li>
 * </ul>
 */
public abstract class AbstractQueryTest {

    private static final Logger log = LoggerFactory.getLogger(AbstractQueryTest.class);

    public enum RangeType {
        DOCUMENT, SHARD
    }

    protected Authorizations auths = new Authorizations("ALL");
    protected Set<Authorizations> authSet = Collections.singleton(auths);

    protected final DateFormat format = new SimpleDateFormat("yyyyMMdd");
    protected final KryoDocumentDeserializer deserializer = new KryoDocumentDeserializer();

    protected final HitTermAssertions hitTermAssertions = new HitTermAssertions();

    protected AccumuloClient clientForTest;

    // variables that support declarative style tests
    private String query;
    private String startDate;
    private String endDate;

    protected final Map<String,String> parameters = new HashMap<>();
    private final Set<String> expected = new HashSet<>();
    private final Set<Document> results = new HashSet<>();

    public abstract ShardQueryLogic getLogic();

    @After
    public void after() {
        query = null;
        startDate = null;
        endDate = null;
        parameters.clear();
        expected.clear();
        results.clear();
    }

    public void withQuery(String query) {
        this.query = query;
    }

    public void withDate(String date) {
        withDate(date, date);
    }

    public void withDate(String startDate, String endDate) {
        this.startDate = startDate;
        this.endDate = endDate;
    }

    /**
     * Required hit terms must exist in every result, for example an anchor term
     *
     * @param hitTerms
     *            one or more hit terms
     */
    public void withRequiredAllOf(String... hitTerms) {
        hitTermAssertions.withRequiredAllOf(hitTerms);
    }

    /**
     * Required hit terms must exist in every result, for example an anchor term
     *
     * @param hitTerms
     *            one or more hit terms
     */
    public void withRequiredAnyOf(String... hitTerms) {
        hitTermAssertions.withRequiredAnyOf(hitTerms);
    }

    public void planAndExecuteQuery() throws Exception {
        planQuery();
        executeQuery();
        assertHitTerms();
    }

    public void planQuery() throws Exception {
        try {
            QueryImpl settings = new QueryImpl();
            settings.setBeginDate(getStartDate());
            settings.setEndDate(getEndDate());
            settings.setPagesize(Integer.MAX_VALUE);
            settings.setQueryAuthorizations(auths.serialize());
            settings.setQuery(getQuery());
            settings.setParameters(parameters);
            settings.setId(UUID.randomUUID());

            getLogic().setMaxEvaluationPipelines(1);
            getLogic().setHitList(true); // always ask for HIT_TERMs

            GenericQueryConfiguration config = getLogic().initialize(clientForTest, settings, authSet);
            getLogic().setupQuery(config);
        } catch (Exception e) {
            log.info("exception while planning query", e);
            throw e;
        }
    }

    public void executeQuery() {
        results.clear();
        for (Map.Entry<Key,Value> entry : getLogic()) {
            Document d = deserializer.apply(entry).getValue();
            results.add(d);
        }
        getLogic().close();
        log.info("query retrieved {} results", results.size());
    }

    public void assertResultCount(int expected) {
        assertEquals(expected, results.size());
    }

    public void assertHitTerms() {
        assertEquals(hitTermAssertions.hitTermExpected(), !results.isEmpty());
        if (!results.isEmpty()) {
            boolean validated = hitTermAssertions.assertHitTerms(results);
            assertEquals(hitTermAssertions.hitTermExpected(), validated);
        }
        hitTermAssertions.resetState();
    }

    public void assertPlannedQuery(String query) {
        try {
            ASTJexlScript expected = JexlASTHelper.parseAndFlattenJexlQuery(query);
            ASTJexlScript plannedScript = getLogic().getConfig().getQueryTree();
            if (!TreeEqualityVisitor.isEqual(expected, plannedScript)) {
                log.info("expected: {}", query);
                log.info("planned : {}", getLogic().getConfig().getQueryString());
                fail("Planned query did not match expectation");
            }
        } catch (ParseException e) {
            fail("Failed to parse query: " + query);
        }
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
}
