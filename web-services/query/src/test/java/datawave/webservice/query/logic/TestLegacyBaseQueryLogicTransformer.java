package datawave.webservice.query.logic;

import datawave.marking.MarkingFunctions;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;
import org.easymock.EasyMock;
import org.easymock.EasyMockExtension;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

@Disabled
@ExtendWith(EasyMockExtension.class)
public class TestLegacyBaseQueryLogicTransformer extends EasyMockSupport {
    
    @Mock
    BaseQueryResponse response;
    
    @Mock
    ResultsPage resultsPage;
    
    @Test
    public void testConstructor_NullVisibilityInterpreter() throws Exception {
        // Run the test
        IllegalArgumentException result1 = assertThrows(IllegalArgumentException.class, () -> new TestTransformer(null, this.response));
        // Verify results
        assertNotNull(result1, "Expected an exception to be thrown due to null param");
    }
    
    @Test
    public void testCreateResponse_ResultsPagePartial() throws Exception {
        // Set expectations
        expect(this.resultsPage.getResults()).andReturn(Arrays.asList(new Object()));
        expect(this.resultsPage.getStatus()).andReturn(ResultsPage.Status.PARTIAL);
        this.response.addMessage(isA(String.class));
        this.response.setPartialResults(true);
        
        // Run the test
        EasyMock.replay();
        TestTransformer subject = new TestTransformer(new MarkingFunctions.Default(), this.response);
        BaseQueryResponse result1 = subject.createResponse(this.resultsPage);
        EasyMock.verify();
        
        // Verify results
        assertSame(result1, this.response, "BaseQueryResponse should not be null");
    }
    
    @Test
    public void testCreateResponse_ResultsPageComplete() throws Exception {
        // Set expectations
        expect(this.resultsPage.getResults()).andReturn(Arrays.asList(new Object()));
        expect(this.resultsPage.getStatus()).andReturn(ResultsPage.Status.COMPLETE);
        
        // Run the test
        EasyMock.replay();
        TestTransformer subject = new TestTransformer(new MarkingFunctions.Default(), this.response);
        BaseQueryResponse result1 = subject.createResponse(this.resultsPage);
        EasyMock.verify();
        
        // Verify results
        assertSame(result1, this.response, "BaseQueryResponse should not be null");
    }
    
    private class TestTransformer extends BaseQueryLogicTransformer<Map.Entry<?,?>,EventBase> {
        BaseQueryResponse response;
        
        public TestTransformer(MarkingFunctions markingFunctions, BaseQueryResponse response) {
            super(markingFunctions);
            this.response = response;
        }
        
        @Override
        public EventBase transform(Map.Entry<?,?> arg0) {
            return null;
        }
        
        @Override
        public BaseQueryResponse createResponse(List<Object> resultList) {
            return this.response;
        }
    }
}
