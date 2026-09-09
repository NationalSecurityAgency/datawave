package datawave.webservice.query.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import datawave.core.query.cache.ResultsPage;
import datawave.core.query.logic.BaseQueryLogicTransformer;
import datawave.marking.MarkingFunctions;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;

@ExtendWith(MockitoExtension.class)
public class TestLegacyBaseQueryLogicTransformer {

    @Mock
    BaseQueryResponse response;

    @Mock
    ResultsPage resultsPage;

    @Test
    public void testConstructor_NullVisibilityInterpreter() {
        // Run the test
        Exception result1 = null;
        try {
            new TestTransformer(null, this.response);
        } catch (IllegalArgumentException e) {
            result1 = e;
        }

        // Verify results
        assertNotNull(result1, "Expected an exception to be thrown due to null param");
    }

    @Test
    public void testCreateResponse_ResultsPagePartial() {
        // Set expectations
        when(this.resultsPage.getResults()).thenReturn(List.of(new Object()));
        when(this.resultsPage.getStatus()).thenReturn(ResultsPage.Status.PARTIAL);
        this.response.addMessage(isA(String.class));
        this.response.setPartialResults(true);

        // Run the test
        TestTransformer subject = new TestTransformer(new MarkingFunctions.Default(), this.response);
        BaseQueryResponse result1 = subject.createResponse(this.resultsPage);

        // Verify results
        assertEquals(result1, this.response, "BaseQueryResponse should not be null");
    }

    @Test
    public void testCreateResponse_ResultsPageComplete() {
        // Set expectations
        when(this.resultsPage.getResults()).thenReturn(List.of(new Object()));
        when(this.resultsPage.getStatus()).thenReturn(ResultsPage.Status.COMPLETE);

        // Run the test
        TestTransformer subject = new TestTransformer(new MarkingFunctions.Default(), this.response);
        BaseQueryResponse result1 = subject.createResponse(this.resultsPage);

        // Verify results
        assertEquals(result1, this.response, "BaseQueryResponse should not be null");
    }

    private static class TestTransformer extends BaseQueryLogicTransformer<Map.Entry<?,?>,EventBase<?,?>> {
        BaseQueryResponse response;

        public TestTransformer(MarkingFunctions<?> markingFunctions, BaseQueryResponse response) {
            super(markingFunctions);
            this.response = response;
        }

        @Override
        public EventBase<?,?> transform(Map.Entry<?,?> arg0) {
            return null;
        }

        @Override
        public BaseQueryResponse createResponse(List<Object> resultList) {
            return this.response;
        }
    }
}
