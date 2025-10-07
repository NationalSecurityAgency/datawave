package datawave.webservice.query;

import static org.junit.Assert.assertEquals;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;
import org.junit.Before;
import org.junit.Test;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryParameters;

public class TestQueryParameters {

    private DefaultQueryParameters qp;
    private MultiValueMap<String,String> parameters;

    @Before
    public void setup() {
        qp = new DefaultQueryParameters();
        parameters = new LinkedMultiValueMap<>();
        parameters.add(QueryParameters.QUERY_AUTHORIZATIONS, "ALL");
        parameters.add(QueryParameters.QUERY_NAME, "Test");
        parameters.add(QueryParameters.QUERY_PERSISTENCE, "TRANSIENT");
        parameters.add(QueryParameters.QUERY_STRING, "FOO == BAR");
        parameters.add(QueryParameters.QUERY_LOGIC_NAME, "LogicName");
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
        parameters.add(QueryParameters.QUERY_EXPIRATION, "+24Hours");
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
        parameters.add(QueryParameters.QUERY_EXPIRATION, expDateString);
        qp.validate(parameters);
        assertEquals(expDateString + " 235959.999", msFormat.format(qp.getExpirationDate()));
    }

    @Test
    public void testTimeExpirationDate() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat msFormat = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String expDateString = format.format(DateUtils.addDays(new Date(), 1));
        parameters.add(QueryParameters.QUERY_EXPIRATION, expDateString);
        qp.validate(parameters);
        assertEquals(expDateString + ".999", msFormat.format(qp.getExpirationDate()));
    }

    @Test
    public void testTimeMillisExpirationDate() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String expDateString = format.format(DateUtils.addDays(new Date(), 1));
        parameters.add(QueryParameters.QUERY_EXPIRATION, expDateString);
        qp.validate(parameters);
        assertEquals(expDateString, format.format(qp.getExpirationDate()));
    }

    @Test
    public void testStartDateNoTime() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat msFormat = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String startDateStr = format.format(new Date());

        parameters.remove(QueryParameters.QUERY_BEGIN);
        parameters.add(QueryParameters.QUERY_BEGIN, startDateStr);
        qp.validate(parameters);
        assertEquals(startDateStr + " 000000.000", msFormat.format(qp.getBeginDate()));
    }

    @Test
    public void testStartDateNoMs() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat msFormat = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String startDateStr = format.format(new Date());

        parameters.remove(QueryParameters.QUERY_BEGIN);
        parameters.add(QueryParameters.QUERY_BEGIN, startDateStr);
        qp.validate(parameters);
        assertEquals(startDateStr + ".000", msFormat.format(qp.getBeginDate()));
    }

    @Test
    public void testEndDateNoTime() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        SimpleDateFormat msFormat = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String endDateStr = format.format(new Date());

        parameters.remove(QueryParameters.QUERY_END);
        parameters.add(QueryParameters.QUERY_END, endDateStr);
        qp.validate(parameters);
        assertEquals(endDateStr + " 235959.999", msFormat.format(qp.getEndDate()));
    }

    @Test
    public void testEndDateNoMs() {
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd HHmmss");
        SimpleDateFormat msFormat = new SimpleDateFormat("yyyyMMdd HHmmss.SSS");
        String endDateStr = format.format(new Date());

        parameters.remove(QueryParameters.QUERY_END);
        parameters.add(QueryParameters.QUERY_END, endDateStr);
        qp.validate(parameters);
        assertEquals(endDateStr + ".999", msFormat.format(qp.getEndDate()));
    }
}
