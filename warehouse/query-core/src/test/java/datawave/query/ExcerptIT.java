package datawave.query;

import static datawave.query.transformer.ExcerptTransform.HIT_EXCERPT;
import static datawave.table.constants.TableName.SHARD_INDEX;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.data.type.LcType;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Content;
import datawave.query.attributes.Document;
import datawave.query.tables.ShardQueryLogic;
import datawave.query.util.AbstractIngest;
import datawave.query.util.AbstractQueryTest;

/**
 * Test excerpts against tf fields, including context style hashes, with and without phrase queries
 */
@ExtendWith(SpringExtension.class)
@ComponentScan(basePackages = {"datawave.configuration.spring", "datawave.query"})
// @formatter:off
@ContextConfiguration(locations = {
        "classpath:datawave/query/QueryLogicFactory.xml",
        "classpath:beanRefContext.xml",
        "classpath:MarkingFunctionsContext.xml",
        "classpath:MetadataHelperContext.xml",
        "classpath:CacheContext.xml"})
// @formatter:on
public class ExcerptIT extends AbstractQueryTest {
    private static final Authorizations auths = new Authorizations("ALL");

    protected static AccumuloClient client = null;

    protected static AbstractIngest ingest = null;

    private List<String> excerpts = new ArrayList<>();

    private void expectExcerpt(String excerpt) {
        excerpts.add(excerpt);
    }

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
    protected List<String> getIndexTableNames() {
        return List.of(SHARD_INDEX);
    }

    @Override
    protected void extraConfigurations() {

    }

    @Override
    protected void extraAssertions() {
        Document d = results.iterator().next();
        Attribute<?> attribute = d.get(HIT_EXCERPT);
        if (excerpts.isEmpty()) {
            assertNull(attribute);
            return;
        }

        assertNotNull(attribute);
        if (excerpts.size() == 1) {
            assertInstanceOf(Content.class, attribute);
            String excerpt = ((Content) attribute).getContent();
            assertEquals(excerpts.get(0), excerpt);
            return;
        }

        assertInstanceOf(Attributes.class, attribute);
        Attributes attributes = (Attributes) attribute;
        assertEquals(excerpts.size(), attributes.size());

        for (Attribute<?> excerptAttribute : attributes.getAttributes()) {
            assertInstanceOf(Content.class, excerptAttribute);
            String excerpt = ((Content) excerptAttribute).getContent();
            boolean removed = excerpts.remove(excerpt);
            assertTrue(removed, "unexpected excerpt: " + excerpt);
        }

        assertEquals(0, excerpts.size());
    }

    @BeforeEach
    public void beforeEach() {
        setClientForTest(client);
    }

    @AfterEach
    public void cleanup() {
        excerpts = new ArrayList<>();
    }

    @BeforeAll
    public static void setupIngest() throws AccumuloSecurityException, AccumuloException {
        InMemoryInstance i = new InMemoryInstance(TermFrequencyContextIT.class.getName());
        client = new InMemoryAccumuloClient("", i);

        ingest = new AbstractIngest(client, auths);
        ingest.registerField("TF", new LcType());
        ingest.registerColumns("TF", List.of("i", "tf"));

        ingest.writeTokenized(1, "TF", "Z A");
        ingest.writeTokenized(1, "TF.123", "X B Y");
        ingest.writeTokenized(1, "TF.234", "0 0 0 0 0 0 0 0 B C");
        ingest.writeTokenized(1, "TF.345", "0 C 0");
        ingest.writeTokenized(1, "TF.456", "C X B");
        ingest.writeTokenized(1, "TF.567", "K K P P T K K");
    }

    @Test
    public void hitTermImpactTest() throws Exception {
        givenDate(ingest.getDate());
        givenParameter(QueryParameters.EXCERPT_FIELDS, "TF/2");
        givenQuery("TF == 'a'");
        expectPlan("TF == 'a'");
        expectResultCount(1);
        expectHitTermsOptionalAnyOf("TF:a");
        expectExcerpt("z [a]");

        planAndExecuteQuery();
    }

    @Test
    public void hitTermFirstMatchTest() throws Exception {
        givenDate(ingest.getDate());
        givenParameter(QueryParameters.EXCERPT_FIELDS, "TF/2");
        givenQuery("TF == 'c'");
        expectPlan("TF == 'c'");
        expectResultCount(1);
        expectHitTermsOptionalAnyOf("TF:c");
        // NOTE: this is the first excerpt hit by hash
        expectExcerpt("0 b [c]");
        // "0 [c] 0" not included because it is not the first match

        planAndExecuteQuery();
    }

    @Test
    public void phraseTest() throws Exception {
        givenDate(ingest.getDate());
        givenParameter(QueryParameters.EXCERPT_FIELDS, "TF/2");
        givenQuery("content:phrase(TF, termOffsetMap, 'b', 'c')");
        // phrase query expands the plan to include independent field checks
        expectPlan("TF == 'b' && TF == 'c' && content:phrase(TF, termOffsetMap, 'b', 'c')");
        expectResultCount(1);
        // b and c independently added to the hit term due to expanded query plan
        expectHitTermsOptionalAnyOf("TF:b", "TF:c", "TF.234:b c");
        // b first occurs in this excerpt, so it is returned
        expectExcerpt("x [b] y");
        // first match on the phrase, and also on c
        expectExcerpt("0 [b] [c]");
        // "0 [c] 0" not included because it is not the first match on c

        planAndExecuteQuery();
    }

    @Test
    public void multiPhraseTest() throws Exception {
        givenDate(ingest.getDate());
        givenParameter(QueryParameters.EXCERPT_FIELDS, "TF/2");
        givenQuery("content:phrase(TF, termOffsetMap, 'x', 'b')");
        // phrase query expands the plan to include independent field checks
        expectPlan("TF == 'x' && TF == 'b' && content:phrase(TF, termOffsetMap, 'x', 'b')");
        expectResultCount(1);
        // there are two phrase hits, and they both get populated
        expectHitTermsOptionalAnyOf("TF:b", "TF:x", "TF.123:x b", "TF.456:x b");

        // this comes from .456
        expectExcerpt("c [x] [b]");
        // this comes from .123
        // this would have come from naked b (first)
        // this would have come from naked x (first)
        expectExcerpt("[x] [b] y");

        planAndExecuteQuery();
    }

    @Disabled
    @Test
    public void testDuplicateCharacterWithExcerpt() throws Exception {
        givenDate(ingest.getDate());
        givenParameter(QueryParameters.EXCERPT_FIELDS, "TF/2");
        givenQuery("content:phrase(TF, termOffsetMap, 'p', 't')");
        // phrase query expands the plan to include independent field checks
        expectPlan("TF == 'p' && TF == 't' && content:phrase(TF, termOffsetMap, 'p', 't')");
        expectResultCount(1);
        // there are two phrase hits, and they both get populated
        expectHitTermsOptionalAnyOf("TF:p", "TF:t", "TF.567:p t");

        // this isn't actually returned
        expectExcerpt("k k p [p] [t] k k");
        // actually returned today "[p] [t] k"

        planAndExecuteQuery();
    }

    @Test
    public void testSimpleIdQuery() throws Exception {
        givenDate(ingest.getDate());
        givenParameter(QueryParameters.EXCERPT_FIELDS, "TF/2");
        givenQuery("TF == 'a' && content:phrase(TF, termOffsetMap, 'b', 'c')");
        expectPlan("TF == 'a' && TF == 'b' && TF == 'c' && content:phrase(TF, termOffsetMap, 'b', 'c')");
        expectResultCount(1);
        expectHitTermsOptionalAnyOf("TF:a", "TF:b", "TF:c", "TF.234:b c");
        expectExcerpt("z [a]");
        expectExcerpt("x [b] y");
        expectExcerpt("0 [b] [c]");

        planAndExecuteQuery();
    }
}
