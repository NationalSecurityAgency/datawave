package datawave.webservice.query;

import java.text.ParseException;
import java.util.Date;
import java.util.UUID;

import javax.ws.rs.core.MultivaluedHashMap;
import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.time.DateUtils;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class QueryParametersTest {
    
    private QueryParameters qp;
    private String auths = "THERE,IS,NO,SPOON,007,DistrictB13,Order66";
    
    private Date beginDate;
    private Date endDate;
    private Date expDate;
    private String logicName;
    private int pagesize;
    private QueryPersistence persistenceMode;
    private String query;
    private String queryName;
    private MultivaluedMap<String,String> requestHeaders;
    private boolean trace;
    
    private long accumuloDate = 1470528000000l; // Accumulo - Aug 7, 2016
    private long nifiDate = 1470614400000l; // NiFi - Aug 8, 2016
    
    private String formatDateCheck = "20160807 000000.000";
    @SuppressWarnings("deprecation")
    private Date parseDateCheck = new Date("Sun Aug 7 00:00:00 GMT 2016");

    private String headerName = "Header-name1";
    private String headerValue = "headervalue1";

    @Before
    public void beforeTests() {
        beginDate = new Date(accumuloDate);
        endDate = new Date(nifiDate);
        expDate = new Date(nifiDate);
        logicName = "QueryTest";
        pagesize = 1;
        persistenceMode = QueryPersistence.PERSISTENT;
        query = "WHEREIS:Waldo";
        queryName = "myQueryForTests";
        requestHeaders = new MultivaluedHashMap<String,String>();
        requestHeaders.add(headerName, headerValue);
        trace = true;
        
        // Build initial QueryParameters
        qp = buildQueryParameters();
    }
    
    private QueryParameters buildQueryParameters() {
        QueryParametersImpl qpBuilder = new QueryParametersImpl();
        qpBuilder.setAuths(auths);
        qpBuilder.setBeginDate(beginDate);
        qpBuilder.setEndDate(endDate);
        qpBuilder.setExpirationDate(expDate);
        qpBuilder.setLogicName(logicName);
        qpBuilder.setPagesize(pagesize);
        qpBuilder.setPersistenceMode(persistenceMode);
        qpBuilder.setQuery(query);
        qpBuilder.setQueryName(queryName);
        qpBuilder.setRequestHeaders(requestHeaders);
        qpBuilder.setTrace(trace);
        
        return qpBuilder;
    }
    
    @Test
    public void testAllTheParams() {
        
        // Validate that query was built correctly
        Assert.assertEquals(auths, qp.getAuths());
        Assert.assertEquals(beginDate, qp.getBeginDate());
        Assert.assertEquals(endDate, qp.getEndDate());
        Assert.assertEquals(expDate, qp.getExpirationDate());
        Assert.assertEquals(logicName, qp.getLogicName());
        Assert.assertEquals(pagesize, qp.getPagesize());
        Assert.assertEquals(persistenceMode, qp.getPersistenceMode());
        Assert.assertEquals(query, qp.getQuery());
        Assert.assertEquals(queryName, qp.getQueryName());
        Assert.assertEquals(requestHeaders, qp.getRequestHeaders());
        Assert.assertEquals(trace, qp.isTrace());
        
        // Store results of hashCode() method, pre-clear
        int hashCode = qp.hashCode();
        
        // Test and validate the QueryParamters.equals(QueryParameters params) method
        QueryParameters carbonCopy = buildQueryParameters();
        Assert.assertEquals(true, qp.equals(carbonCopy));
        
        // Test and validate date formatting, parsing
        try {
            Assert.assertEquals(formatDateCheck, QueryParametersImpl.formatDate(beginDate));
            Assert.assertEquals(parseDateCheck, QueryParametersImpl.parseStartDate(QueryParametersImpl.formatDate(beginDate)));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        
        // Test the QueryParametersImpl.validate(QueryParametersImpl params) method
        MultivaluedMap<String,String> params = new MultivaluedMapImpl<String,String>();
        
        // Reset the MulivaluedMap for a QueryParametersImpl.validate() call
        try {
            params.add(QueryParameters.QUERY_STRING, "string");
            params.add(QueryParameters.QUERY_NAME, "name");
            params.add(QueryParameters.QUERY_PERSISTENCE, "PERSISTENT");
            params.add(QueryParameters.QUERY_PAGESIZE, "10");
            params.add(QueryParameters.QUERY_AUTHORIZATIONS, "auths");
            params.add(QueryParameters.QUERY_EXPIRATION, QueryParametersImpl.formatDate(expDate).toString());
            params.add(QueryParameters.QUERY_TRACE, "trace");
            params.add(QueryParameters.QUERY_BEGIN, QueryParametersImpl.formatDate(beginDate).toString());
            params.add(QueryParameters.QUERY_END, QueryParametersImpl.formatDate(endDate).toString());
            params.add(QueryParameters.QUERY_PARAMS, "params");
            params.add(QueryParameters.QUERY_LOGIC_NAME, "logicName");
        } catch (ParseException e) {
            e.printStackTrace();
        }
        
        // Add an unknown parameter
        params.add("key", "value");
        
        MultivaluedMap<String,String> unknownParams = qp.getUnknownParameters(params);
        Assert.assertEquals("params", unknownParams.getFirst(QueryParameters.QUERY_PARAMS));
        Assert.assertEquals("value", unknownParams.getFirst("key"));
        Assert.assertEquals(2, unknownParams.size());
        
        // Test the QueryParameters.validate() method
        qp.validate(params);
        
        // Test and validate the QueryParameters.clear() method
        Date start = new Date();
        qp.clear();
        Date end = new Date();
        long delta = end.getTime() - start.getTime();
        
        Assert.assertEquals(null, qp.getAuths());
        Assert.assertEquals(null, qp.getBeginDate());
        Assert.assertEquals(null, qp.getEndDate());
        Assert.assertTrue(qp.getExpirationDate().getTime() - DateUtils.addDays(start, 1).getTime() <= delta);
        Assert.assertEquals(null, qp.getLogicName());
        Assert.assertEquals(10, qp.getPagesize());
        Assert.assertEquals(QueryPersistence.TRANSIENT, qp.getPersistenceMode());
        Assert.assertEquals(null, qp.getQuery());
        Assert.assertEquals(null, qp.getQueryName());
        Assert.assertEquals(null, qp.getRequestHeaders());
        Assert.assertEquals(false, qp.isTrace());
        
        // Reset a few variables so hashCode() doesn't blow up, then
        // store results of hashCode() method, post-clear
        qp.setQuery(query);
        qp.setQueryName(queryName);
        qp.setPersistenceMode(persistenceMode);
        qp.setAuths(auths);
        qp.setExpirationDate(expDate);
        
        int hashCodePostClear = qp.hashCode();
        Assert.assertNotEquals(hashCode, hashCodePostClear);
    }
}
