package datawave.webservice.query.logic;

import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.api.easymock.annotation.Mock;
import org.powermock.modules.junit4.PowerMockRunner;

import datawave.marking.MarkingFunctions;
import datawave.webservice.query.cache.ResultsPage;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.result.BaseQueryResponse;

@RunWith(PowerMockRunner.class)
public class TestLegacyBaseQueryLogicTransformer {

    @Mock
    BaseQueryResponse response;

    @Mock
    ResultsPage resultsPage;

    @Test
    public void testConstructor_NullVisibilityInterpreter() throws Exception {
        // Run the test
        Exception result1 = null;
        try {
            new TestTransformer(null, this.response);
        } catch (IllegalArgumentException e) {
            result1 = e;
        }

        // Verify results
        assertNotNull("Expected an exception to be thrown due to null param", result1);
    }

    @Test
    public void testCreateResponse_ResultsPagePartial() throws Exception {
        // Set expectations
        expect(this.resultsPage.getResults()).andReturn(Arrays.asList(new Object()));
        expect(this.resultsPage.getStatus()).andReturn(ResultsPage.Status.PARTIAL);
        this.response.addMessage(isA(String.class));
        this.response.setPartialResults(true);

        // Run the test
        PowerMock.replayAll();
        TestTransformer subject = new TestTransformer(new MarkingFunctions.Default(), this.response);
        BaseQueryResponse result1 = subject.createResponse(this.resultsPage);
        PowerMock.verifyAll();

        // Verify results
        assertSame("BaseQueryResponse should not be null", result1, this.response);
    }

    @Test
    public void testCreateResponse_ResultsPageComplete() throws Exception {
        // Set expectations
        expect(this.resultsPage.getResults()).andReturn(Arrays.asList(new Object()));
        expect(this.resultsPage.getStatus()).andReturn(ResultsPage.Status.COMPLETE);

        // Run the test
        PowerMock.replayAll();
        TestTransformer subject = new TestTransformer(new MarkingFunctions.Default(), this.response);
        BaseQueryResponse result1 = subject.createResponse(this.resultsPage);
        PowerMock.verifyAll();

        // Verify results
        assertSame("BaseQueryResponse should not be null", result1, this.response);
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
