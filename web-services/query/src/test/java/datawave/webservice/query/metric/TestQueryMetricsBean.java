package datawave.webservice.query.metric;

import static org.easymock.EasyMock.anyBoolean;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;

import java.util.Date;

import javax.ejb.EJBContext;

import datawave.security.authorization.DatawavePrincipal;

import org.easymock.EasyMockRunner;
import org.easymock.EasyMockSupport;
import org.easymock.Mock;
import org.easymock.TestSubject;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class TestQueryMetricsBean extends EasyMockSupport {
    @TestSubject
    QueryMetricsBean queryMetricsBean = new QueryMetricsBean();
    @Mock
    DatawavePrincipal callerPrincipal;
    @Mock
    EJBContext ejbContext;
    @Mock
    QueryMetricListResponse listResponse;
    
    @Mock
    QueryMetricHandler queryMetricHandler;
    @Mock
    QueryMetric metric;
    @Mock
    QueryMetricsSummaryHtmlResponse summaryResponse;
    
    @Test
    public void testQuery_TotalQueriesSummary() throws Exception {
        // Set local test arguments
        Date beginDate = new Date(System.currentTimeMillis() - 2000);
        Date endDate = new Date(System.currentTimeMillis() - 1000);
        
        // Set expectations
        expect(this.ejbContext.getCallerPrincipal()).andReturn(this.callerPrincipal);
        expect(this.queryMetricHandler.getTotalQueriesSummary(isA(Date.class), isA(Date.class), isA(DatawavePrincipal.class))).andReturn(this.summaryResponse);
        
        // Run the test
        replayAll();
        QueryMetricsSummaryResponse result1 = queryMetricsBean.getQueryMetricsSummary(beginDate, endDate);
        verifyAll();
        
        // Verify results
        assertSame("Query result should not be null", this.summaryResponse, result1);
    }
    
    @Test
    public void testQuery_ByQueryId() throws Exception {
        // Set local test arguments
        String sid = "usersid";
        String queryId = "usersid";
        
        // Set expectations
        expect(this.ejbContext.getCallerPrincipal()).andReturn(this.callerPrincipal);
        expect(this.callerPrincipal.getName()).andReturn("userdn");
        expect(this.callerPrincipal.getShortName()).andReturn(sid);
        expect(this.queryMetricHandler.query(sid, queryId, this.callerPrincipal)).andReturn(this.listResponse);
        
        // Run the test
        replayAll();
        BaseQueryMetricListResponse result1 = queryMetricsBean.query(queryId);
        verifyAll();
        
        // Verify results
        assertNotNull("Query result should not be null", result1);
    }
}
