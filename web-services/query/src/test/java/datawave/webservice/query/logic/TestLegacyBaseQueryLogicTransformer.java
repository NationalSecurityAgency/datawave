package datawave.webservice.query.logic;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;

@ExtendWith(MockitoExtension.class)
public class TestLegacyBaseQueryLogicTransformer {

    @Mock
    BaseQueryResponse response;

    @Mock
    ResultsPage resultsPage;

    @Test
    public void testCreateResponse_ResultsPagePartial() {
        // Set expectations
        when(this.resultsPage.getResults()).thenReturn(List.of(new Object()));
        when(this.resultsPage.getStatus()).thenReturn(ResultsPage.Status.PARTIAL);
        this.response.addMessage(isA(String.class));
        this.response.setPartialResults(true);

        // Run the test
        TestTransformer subject = new TestTransformer(this.response);
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
        TestTransformer subject = new TestTransformer(this.response);
        BaseQueryResponse result1 = subject.createResponse(this.resultsPage);

        // Verify results
        assertEquals(result1, this.response, "BaseQueryResponse should not be null");
    }

    private static class TestTransformer extends BaseQueryLogicTransformer<Map.Entry<?,?>,EventBase<?,?>> {
        BaseQueryResponse response;

        public TestTransformer(BaseQueryResponse response) {
            super();
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
