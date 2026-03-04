package datawave.query.transformer.annotation.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AllHitsTest {
    private ObjectMapper mapper;
    private AllHits allHits;

    @BeforeEach
    public void setup() {
        mapper = new ObjectMapper();
        allHits = new AllHits();
    }

    @Test
    public void testNoImpactDynamicEmpty() throws JsonProcessingException {
        assertEquals("{\"annotationId\":null,\"maxTermHitConfidence\":0.0,\"keywordResultList\":[]}", mapper.writeValueAsString(allHits));
    }

    /**
     * verify properties added to the dynamic map are serialized into the top level json
     *
     * @throws JsonProcessingException
     */
    @Test
    public void testDynamicFields() throws JsonProcessingException {
        allHits.getDynamicProperties().put("test", "abc");
        assertEquals("{\"annotationId\":null,\"maxTermHitConfidence\":0.0,\"keywordResultList\":[],\"test\":\"abc\"}", mapper.writeValueAsString(allHits));
    }

    /**
     * verify top level properties will be put back into the dynamic map when serialized
     *
     * @throws JsonProcessingException
     */
    @Test
    public void testSetDynamicFields() throws JsonProcessingException {
        AllHits allHits = mapper.readValue("{\"annotationId\":null,\"maxTermHitConfidence\":0.0,\"keywordResultList\":[],\"test\":\"abc\"}", AllHits.class);
        assertEquals("abc", allHits.getDynamicProperties().get("test"));
    }

    /**
     * Verify cannot set a dynamic property that matches a real property
     */
    // @formatter:off
    @CsvSource({
        "maxTermHitConfidence,3",
        "annotationId,123",
        "keywordResultList,[]",
        "dynamicProperties,{}"
    })
    // @formatter:on
    @ParameterizedTest
    public void testIllegalDynamicProperty(String key, String value) {
        assertThrows(IllegalArgumentException.class, () -> allHits.addDynamicProperties(key, value));
    }
}
