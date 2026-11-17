package datawave.query.tables.ssdeep;

import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.EXPECTED_2_3_OVERLAPS;
import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.TEST_SSDEEPS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctionsFactory;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.util.ssdeep.NGramGenerator;
import datawave.util.ssdeep.NGramTuple;
import datawave.util.ssdeep.SSDeepHash;
import datawave.util.ssdeep.SSDeepHashEditDistanceScorer;
import datawave.util.ssdeep.SSDeepHashScorer;
import datawave.util.ssdeep.SSDeepNGramOverlapScorer;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;

public class SSDeepSimilarityQueryTransformerTest {

    private Query query;
    private final MarkingFunctions markingFunctions = MarkingFunctionsFactory.createMarkingFunctions();
    private final ResponseObjectFactory responseObjectFactory = new DefaultResponseObjectFactory();

    private SSDeepSimilarityQueryConfiguration config;

    private final Set<String> expectedFields = new HashSet<>();

    @BeforeEach
    public void beforeEach() {
        query = new QueryImpl();
        query.setQueryAuthorizations("A,B,C");

        config = new SSDeepSimilarityQueryConfiguration();

        expectedFields.add("MATCHING_SSDEEP");
        expectedFields.add("QUERY_SSDEEP");
        expectedFields.add("WEIGHTED_SCORE");
        expectedFields.add("OVERLAP_SCORE");
        expectedFields.add("OVERLAP_SSDEEP_NGRAMS");
    }

    @Test
    public void transformTest() {

        final SSDeepHashScorer<Set<NGramTuple>> ngramOverlapScorer = new SSDeepNGramOverlapScorer(NGramGenerator.DEFAULT_NGRAM_SIZE);
        final SSDeepHashScorer<Integer> editDistanceScorer = new SSDeepHashEditDistanceScorer(SSDeepHash.DEFAULT_MAX_REPEATED_CHARACTERS);

        final SSDeepHash query = SSDeepHash.parse(TEST_SSDEEPS[2]);
        final SSDeepHash match = SSDeepHash.parse(TEST_SSDEEPS[3]);
        final Set<NGramTuple> overlappingNGrams = ngramOverlapScorer.apply(query, match);
        final Integer editDistance = editDistanceScorer.apply(query, match);
        final ScoredSSDeepPair scoredSSDeepPair = new ScoredSSDeepPair(query, match, overlappingNGrams, editDistance);

        SSDeepSimilarityQueryTransformer transformer = new SSDeepSimilarityQueryTransformer(this.query, config, markingFunctions, responseObjectFactory);
        EventBase transformedEvent = transformer.transform(scoredSSDeepPair);
        List<Object> resultList = new ArrayList<>();
        resultList.add(transformedEvent);
        BaseQueryResponse baseQueryResponse = transformer.createResponse(resultList);

        assertNotNull(transformedEvent);

        assertInstanceOf(DefaultEventQueryResponse.class, baseQueryResponse);
        DefaultEventQueryResponse defaultEventQueryResponse = (DefaultEventQueryResponse) baseQueryResponse;

        assertEquals(1, defaultEventQueryResponse.getEvents().size());

        EventBase eventBase = defaultEventQueryResponse.getEvents().iterator().next();
        assertInstanceOf(DefaultEvent.class, eventBase);
        DefaultEvent defaultEvent = (DefaultEvent) eventBase;

        List<DefaultField> fields = defaultEvent.getFields();
        for (DefaultField field : fields) {
            assertTrue(expectedFields.remove(field.getName()), "Unexpected field: " + field.getName());
            switch (field.getName()) {
                case "MATCHING_SSDEEP":
                    assertEquals(TEST_SSDEEPS[3], field.getValueString());
                    break;
                case "QUERY_SSDEEP":
                    assertEquals(TEST_SSDEEPS[2], field.getValueString());
                    break;
                case "WEIGHTED_SCORE":
                    assertEquals("96", field.getValueString());
                    break;
                case "OVERLAP_SCORE":
                    assertEquals("53", field.getValueString());
                    break;
                case "OVERLAP_SSDEEP_NGRAMS":
                    assertEquals(EXPECTED_2_3_OVERLAPS, field.getValueString());
                    break;
                default:
                    fail("Unexpected field: " + field.getName());
            }
        }
        assertEquals(5, fields.size());
        assertTrue(expectedFields.isEmpty(), "Did not observe all expected fields: " + expectedFields);
    }
}
