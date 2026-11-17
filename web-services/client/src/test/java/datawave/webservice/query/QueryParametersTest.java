package datawave.webservice.query;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryParameters;
import datawave.microservice.query.QueryPersistence;

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
    private MultiValueMap<String,String> requestHeaders;
    private boolean trace;

    private long accumuloDate = 1470528000000l; // Accumulo - Aug 7, 2016
    private long nifiDate = 1470614400000l; // NiFi - Aug 8, 2016

    private String formatDateCheck = "20160807 000000.000";
    // Aug 7, 2016 00:00:00 GMT (same as accumuloDate)
    private Date parseDateCheck = new Date(1470528000000L);

    private String headerName = "Header-name1";
    private String headerValue = "headervalue1";

    private static final Logger log = Logger.getLogger(QueryParametersTest.class);

    @BeforeEach
    public void beforeTests() {
        beginDate = new Date(accumuloDate);
        endDate = new Date(nifiDate);
        expDate = new Date(nifiDate);
        logicName = "QueryTest";
        pagesize = 1;
        persistenceMode = QueryPersistence.PERSISTENT;
        query = "WHEREIS:Waldo";
        queryName = "myQueryForTests";
        requestHeaders = new LinkedMultiValueMap<>();
        requestHeaders.add(headerName, headerValue);
        trace = true;

        // Build initial QueryParameters
        qp = buildQueryParameters();
    }

    private QueryParameters buildQueryParameters() {
        DefaultQueryParameters qpBuilder = new DefaultQueryParameters();
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
        assertEquals(auths, qp.getAuths());
        assertEquals(beginDate, qp.getBeginDate());
        assertEquals(endDate, qp.getEndDate());
        assertEquals(expDate, qp.getExpirationDate());
        assertEquals(logicName, qp.getLogicName());
        assertEquals(pagesize, qp.getPagesize());
        assertEquals(persistenceMode, qp.getPersistenceMode());
        assertEquals(query, qp.getQuery());
        assertEquals(queryName, qp.getQueryName());
        assertEquals(requestHeaders, qp.getRequestHeaders());
        assertEquals(trace, qp.isTrace());

        // Store results of hashCode() method, pre-clear
        int hashCode = qp.hashCode();

        // Test and validate the QueryParamters.equals(QueryParameters params) method
        QueryParameters carbonCopy = buildQueryParameters();
        assertTrue(qp.equals(carbonCopy));

        // Test and validate date formatting, parsing
        try {
            assertEquals(formatDateCheck, DefaultQueryParameters.formatDate(beginDate));
            assertEquals(parseDateCheck, DefaultQueryParameters.parseStartDate(DefaultQueryParameters.formatDate(beginDate)));
        } catch (ParseException e) {
            log.error(e);
        }

        // Test the QueryParametersImpl.validate(QueryParametersImpl params) method
        MultiValueMap<String,String> params = new LinkedMultiValueMap<>();

        // Reset the MulivaluedMap for a QueryParametersImpl.validate() call
        try {
            params.add(QueryParameters.QUERY_STRING, "string");
            params.add(QueryParameters.QUERY_NAME, "name");
            params.add(QueryParameters.QUERY_PERSISTENCE, "PERSISTENT");
            params.add(QueryParameters.QUERY_PAGESIZE, "10");
            params.add(QueryParameters.QUERY_AUTHORIZATIONS, "auths");
            params.add(QueryParameters.QUERY_EXPIRATION, DefaultQueryParameters.formatDate(expDate).toString());
            params.add(QueryParameters.QUERY_TRACE, "trace");
            params.add(QueryParameters.QUERY_BEGIN, DefaultQueryParameters.formatDate(beginDate).toString());
            params.add(QueryParameters.QUERY_END, DefaultQueryParameters.formatDate(endDate).toString());
            params.add(QueryParameters.QUERY_PARAMS, "params");
            params.add(QueryParameters.QUERY_LOGIC_NAME, "logicName");
        } catch (ParseException e) {
            log.error(e);
        }

        // Add an unknown parameter
        params.add("key", "value");

        MultiValueMap<String,String> unknownParams = new LinkedMultiValueMap<>();
        unknownParams.putAll(qp.getUnknownParameters(params));
        assertEquals("params", unknownParams.getFirst(QueryParameters.QUERY_PARAMS));
        assertEquals("value", unknownParams.getFirst("key"));
        assertEquals(2, unknownParams.size());

        // Test the QueryParameters.validate() method
        qp.validate(params);

        // Test and validate the QueryParameters.clear() method
        Date start = new Date();
        qp.clear();
        Date end = new Date();
        long delta = end.getTime() - start.getTime();

        assertNull(qp.getAuths());
        assertNull(qp.getBeginDate());
        assertNull(qp.getEndDate());
        assertTrue(qp.getExpirationDate().getTime() - DateUtils.addDays(start, 1).getTime() <= delta);
        assertNull(qp.getLogicName());
        assertEquals(10, qp.getPagesize());
        assertEquals(QueryPersistence.TRANSIENT, qp.getPersistenceMode());
        assertNull(qp.getQuery());
        assertNull(qp.getQueryName());
        assertNull(qp.getRequestHeaders());
        assertFalse(qp.isTrace());

        // Reset a few variables so hashCode() doesn't blow up, then
        // store results of hashCode() method, post-clear
        qp.setQuery(query);
        qp.setQueryName(queryName);
        qp.setPersistenceMode(persistenceMode);
        qp.setAuths(auths);
        qp.setExpirationDate(expDate);

        int hashCodePostClear = qp.hashCode();
        assertNotEquals(hashCode, hashCodePostClear);
    }
}
