package datawave.query.tables.ssdeep;

import static datawave.query.tables.ssdeep.util.SSDeepTestUtil.TEST_SSDEEPS;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.easymock.EasyMock;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import datawave.marking.MarkingFunctions;
import datawave.query.config.SSDeepSimilarityQueryConfiguration;
import datawave.util.ssdeep.SSDeepHash;
import datawave.webservice.query.Query;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.ResponseObjectFactory;
import datawave.webservice.result.BaseQueryResponse;
import datawave.webservice.result.DefaultEventQueryResponse;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*"})
public class SSDeepSimilarityQueryTransformerTest {

    @Mock
    SSDeepSimilarityQueryConfiguration mockConfig;

    @Mock
    private Query mockQuery;

    @Mock
    private MarkingFunctions mockMarkingFunctions;

    @Mock
    private ResponseObjectFactory mockResponseFactory;

    private final Set<String> expectedFields = new HashSet<>();

    public void basicExpects() {
        EasyMock.expect(mockQuery.getQueryAuthorizations()).andReturn("A,B,C");
        EasyMock.expect(mockResponseFactory.getEventQueryResponse()).andAnswer(DefaultEventQueryResponse::new);
        EasyMock.expect(mockResponseFactory.getEvent()).andAnswer(DefaultEvent::new).times(1);
        EasyMock.expect(mockResponseFactory.getField()).andAnswer(DefaultField::new).times(4);

        expectedFields.add("MATCHING_SSDEEP");
        expectedFields.add("QUERY_SSDEEP");
        expectedFields.add("MATCH_SCORE");
        expectedFields.add("WEIGHTED_SCORE");
    }

    @Test
    public void transformTest() {

        final SSDeepHash query = SSDeepHash.parse(TEST_SSDEEPS[2]);
        final SSDeepHash match = SSDeepHash.parse(TEST_SSDEEPS[3]);
        final ScoredSSDeepPair scoredSSDeepPair = new ScoredSSDeepPair(query, match, 51, 96);

        basicExpects();

        PowerMock.replayAll();

        SSDeepSimilarityQueryTransformer transformer = new SSDeepSimilarityQueryTransformer(mockQuery, mockConfig, mockMarkingFunctions, mockResponseFactory);
        EventBase transformedEvent = transformer.transform(scoredSSDeepPair);
        List<Object> resultList = new ArrayList<>();
        resultList.add(transformedEvent);
        BaseQueryResponse baseQueryResponse = transformer.createResponse(resultList);

        PowerMock.verifyAll();

        assertNotNull(transformedEvent);

        assertTrue(baseQueryResponse instanceof DefaultEventQueryResponse);
        DefaultEventQueryResponse defaultEventQueryResponse = (DefaultEventQueryResponse) baseQueryResponse;

        assertEquals(1, defaultEventQueryResponse.getEvents().size());

        EventBase eventBase = defaultEventQueryResponse.getEvents().iterator().next();
        assertTrue(eventBase instanceof DefaultEvent);
        DefaultEvent defaultEvent = (DefaultEvent) eventBase;

        List<DefaultField> fields = defaultEvent.getFields();
        assertEquals(4, fields.size());
        for (DefaultField field : fields) {
            assertTrue("Unexpected field: " + field.getName(), expectedFields.remove(field.getName()));
            switch (field.getName()) {
                case "MATCHING_SSDEEP":
                    assertEquals(TEST_SSDEEPS[3], field.getValueString());
                    break;
                case "QUERY_SSDEEP":
                    assertEquals(TEST_SSDEEPS[2], field.getValueString());
                    break;
                case "MATCH_SCORE":
                    assertEquals("51", field.getValueString());
                    break;
                case "WEIGHTED_SCORE":
                    assertEquals("96", field.getValueString());
                    break;
                default:
                    fail("Unexpected field: " + field.getName());
            }

        }
        assertTrue("Did not observe all expected fields: " + expectedFields, expectedFields.isEmpty());
    }
}
