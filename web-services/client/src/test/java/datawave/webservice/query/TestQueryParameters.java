package datawave.webservice.query;

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.ws.rs.core.MultivaluedMap;

import org.apache.commons.lang.time.DateUtils;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;
import org.junit.Before;
import org.junit.Test;

public class TestQueryParameters {
    
    private QueryParametersImpl qp;
    private MultivaluedMap<String,String> parameters;
    
    @Before
    public void setup() {
        qp = new QueryParametersImpl();
        parameters = new MultivaluedMapImpl<String,String>();
        parameters.putSingle(QueryParameters.QUERY_AUTHORIZATIONS, "ALL");
        parameters.putSingle(QueryParameters.QUERY_NAME, "Test");
        parameters.putSingle(QueryParameters.QUERY_PERSISTENCE, "TRANSIENT");
        parameters.putSingle(QueryParameters.QUERY_STRING, "FOO == BAR");
        parameters.putSingle(QueryParameters.QUERY_LOGIC_NAME, "LogicName");
    }
    
    @Test
    public void testNullExpirationDate() {
        qp.validate(parameters);
        
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HHmmss");
        String expDateString = format.format(DateUtils.addDays(new Date(), 1));
        assertEquals(expDateString, format.format(qp.getExpirationDate()));
    }
    
    @Test
    public void test24HoursExpirationDate() {
        parameters.putSingle(QueryParameters.QUERY_EXPIRATION, "+24Hours");
        qp.validate(parameters);
        
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HHmmss");
        String expDateString = format.format(DateUtils.addDays(new Date(), 1));
        assertEquals(expDateString, format.format(qp.getExpirationDate()));
    }
    
    @Test
    public void testDaysExpirationDate() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat msFormat = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String expDateString = format.format(DateUtils.addDays(new Date(), 1));
        parameters.putSingle(QueryParameters.QUERY_EXPIRATION, expDateString);
        qp.validate(parameters);
        assertEquals(expDateString + " 235959.999", msFormat.format(qp.getExpirationDate()));
    }
    
    @Test
    public void testTimeExpirationDate() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat msFormat = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String expDateString = format.format(DateUtils.addDays(new Date(), 1));
        parameters.putSingle(QueryParameters.QUERY_EXPIRATION, expDateString);
        qp.validate(parameters);
        assertEquals(expDateString + ".999", msFormat.format(qp.getExpirationDate()));
    }
    
    @Test
    public void testTimeMillisExpirationDate() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String expDateString = format.format(DateUtils.addDays(new Date(), 1));
        parameters.putSingle(QueryParameters.QUERY_EXPIRATION, expDateString);
        qp.validate(parameters);
        assertEquals(expDateString, format.format(qp.getExpirationDate()));
    }
    
    @Test
    public void testStartDateNoTime() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat msFormat = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String startDateStr = format.format(new Date());
        
        parameters.remove(QueryParameters.QUERY_BEGIN);
        parameters.putSingle(QueryParameters.QUERY_BEGIN, startDateStr);
        qp.validate(parameters);
        assertEquals(startDateStr + " 000000.000", msFormat.format(qp.getBeginDate()));
    }
    
    @Test
    public void testStartDateNoMs() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat msFormat = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String startDateStr = format.format(new Date());
        
        parameters.remove(QueryParameters.QUERY_BEGIN);
        parameters.putSingle(QueryParameters.QUERY_BEGIN, startDateStr);
        qp.validate(parameters);
        assertEquals(startDateStr + ".000", msFormat.format(qp.getBeginDate()));
    }
    
    @Test
    public void testEndDateNoTime() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat msFormat = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String endDateStr = format.format(new Date());
        
        parameters.remove(QueryParameters.QUERY_END);
        parameters.putSingle(QueryParameters.QUERY_END, endDateStr);
        qp.validate(parameters);
        assertEquals(endDateStr + " 235959.999", msFormat.format(qp.getEndDate()));
    }
    
    @Test
    public void testEndDateNoMs() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat msFormat = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String endDateStr = format.format(new Date());
        
        parameters.remove(QueryParameters.QUERY_END);
        parameters.putSingle(QueryParameters.QUERY_END, endDateStr);
        qp.validate(parameters);
        assertEquals(endDateStr + ".999", msFormat.format(qp.getEndDate()));
    }
}
