package datawave.webservice.query.logic.filtered;

import static org.easymock.EasyMock.capture;
import static org.easymock.EasyMock.eq;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.expectLastCall;
import static org.easymock.EasyMock.isA;
import static org.junit.Assert.assertEquals;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.composite.CompositeQueryLogic;
import datawave.security.authorization.AuthorizationException;
import org.apache.accumulo.core.security.Authorizations;
import org.easymock.Capture;
import org.easymock.EasyMockSupport;
import org.junit.Before;
import org.junit.Test;

import datawave.core.query.logic.QueryLogic;
import datawave.core.query.logic.filtered.DateRangeFilteredQueryLogic;
import datawave.core.query.logic.filtered.QueryLogicFilterByDate;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;

public class DateRangeFilteredQueryLogicTest extends EasyMockSupport {
    private DateRangeFilteredQueryLogic logic;
    private QueryLogic delegate;

    @Before
    public void setup() {
        delegate = createMock(QueryLogic.class);
        logic = new DateRangeFilteredQueryLogic();
        logic.setDelegate(delegate);
    }

    @Test
    public void testDateAdjustedWhenRunning() throws Exception {
        QueryLogicFilterByDate filter = new QueryLogicFilterByDate();

        Calendar c = Calendar.getInstance();
        c.set(2024, Calendar.DECEMBER, 5, 0, 0, 0);

        Date startDate = new Date(c.getTimeInMillis());
        c.set(2024, Calendar.DECEMBER, 5, 23, 59, 59);
        Date endDate = new Date(c.getTimeInMillis());

        filter.setStartDate(startDate);
        filter.setEndDate(endDate);

        logic.setFilter(filter);

        Query settings = new QueryImpl();
        c.set(2023, Calendar.JANUARY, 1, 0, 0, 0);
        settings.setBeginDate(c.getTime());
        c.set(2024, Calendar.DECEMBER, 31, 0, 0, 0);
        settings.setEndDate(c.getTime());

        Capture<QueryImpl> implCapture = Capture.newInstance();

        expect(delegate.initialize(eq(null), capture(implCapture), eq(null))).andReturn(null);

        replayAll();

        logic.initialize(null, settings, null);

        // verify the dates have been adjusted down to the filter
        assertEquals(settings.getBeginDate(), filter.getStartDate());
        assertEquals(settings.getBeginDate(), implCapture.getValue().getBeginDate());
        assertEquals(settings.getEndDate(), filter.getEndDate());
        assertEquals(settings.getEndDate(), implCapture.getValue().getEndDate());

        verifyAll();
    }

    @Test
    public void integrationTest() throws Exception {
        QueryLogic delegate2 = createMock(QueryLogic.class);

        CompositeQueryLogic cql = new CompositeQueryLogicWithoutAuthChecks();

        DateRangeFilteredQueryLogic logic1 = new DateRangeFilteredQueryLogic();
        logic1.setDelegate(delegate);
        logic1.setFilter(createFilter(null, "20241204 235959"));

        DateRangeFilteredQueryLogic logic2 = new DateRangeFilteredQueryLogic();
        logic2.setDelegate(delegate2);
        logic2.setFilter(createFilter("20241205 000000", null));

        cql.setQueryLogics(Map.of("orig", logic1, "new", logic2));

        QueryImpl settings = new QueryImpl();
        settings.setId(UUID.randomUUID());
        settings.setBeginDate(getDate("20230101 010100"));
        settings.setEndDate(getDate("20241205 235959"));
        settings.setPagesize(100);

        Capture<QueryImpl> logic1Settings = Capture.newInstance();
        Capture<QueryImpl> logic1SettingsDupe1 = Capture.newInstance();
        Capture<QueryImpl> logic1SettingsDupe2 = Capture.newInstance();
        Capture<QueryImpl> logic2Settings = Capture.newInstance();
        Capture<QueryImpl> logic2SettingsDupe1 = Capture.newInstance();
        Capture<QueryImpl> logic2SettingsDupe2 = Capture.newInstance();

        GenericQueryConfiguration config1 = new GenericQueryConfiguration();
        config1.setBeginDate(getDate("20230101 010100"));
        config1.setEndDate(getDate("20241204 235959"));
        GenericQueryConfiguration config2 = new GenericQueryConfiguration();
        config2.setBeginDate(getDate("20241205 000000"));
        config2.setEndDate(getDate("20241205 235959"));

        expect(delegate.initialize(eq(null), capture(logic1Settings), eq(Collections.emptySet()))).andReturn(config1);
        expect(delegate.getResultLimit(capture(logic1SettingsDupe1))).andReturn(100l);
        delegate.setupQuery(config1);
        expect(delegate.getTransformIterator(capture(logic1SettingsDupe2))).andReturn(null);

        expect(delegate2.initialize(eq(null), capture(logic2Settings), eq(Collections.emptySet()))).andReturn(config2);
        expect(delegate2.getResultLimit(capture(logic2SettingsDupe1))).andReturn(100l);
        delegate2.setupQuery(config2);
        expect(delegate2.getTransformIterator(capture(logic2SettingsDupe2))).andReturn(null);

        replayAll();

        GenericQueryConfiguration initializedConfig = cql.initialize(null, settings, null);

        // verify updated settings are propagated through the calls
        assertEquals(logic1Settings.getValue(), logic1SettingsDupe1.getValue());
        assertEquals(logic2Settings.getValue(), logic2SettingsDupe1.getValue());

        cql.setupQuery(initializedConfig);

        // verify the same settings are used when setting up the query
        assertEquals(logic2SettingsDupe2.getValue(), logic2SettingsDupe1.getValue());
        assertEquals(logic1SettingsDupe2.getValue(), logic1SettingsDupe1.getValue());

        verifyAll();
    }

    private Date getDate(String val) throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd hhmmss");

        return sdf.parse(val);
    }

    private QueryLogicFilterByDate createFilter(String start, String end) throws ParseException {
        Date startDate = null;
        if (start != null) {
            startDate = getDate(start);
        }
        Date endDate = null;
        if (end != null) {
            endDate = getDate(end);
        }

        QueryLogicFilterByDate filter = new QueryLogicFilterByDate();
        filter.setStartDate(startDate);
        filter.setEndDate(endDate);

        return filter;
    }

    // this complicates the testing, so stub it out
    private static class CompositeQueryLogicWithoutAuthChecks extends CompositeQueryLogic {
        @Override
        public Set<Authorizations> updateRuntimeAuthorizationsAndQueryAuths(QueryLogic<?> logic, Query settings) throws AuthorizationException {
            // no-op
            return Collections.emptySet();
        }
    }
}
