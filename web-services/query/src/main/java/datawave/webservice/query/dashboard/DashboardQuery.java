package datawave.webservice.query.dashboard;

import datawave.webservice.common.extjs.ExtJsResponse;
import datawave.webservice.query.QueryParametersImpl;
import datawave.webservice.query.QueryPersistence;
import datawave.webservice.query.runner.QueryExecutor;
import org.apache.commons.lang.time.DateUtils;
import org.jboss.resteasy.specimpl.MultivaluedMapImpl;

import javax.ws.rs.core.MultivaluedMap;
import java.text.ParseException;
import java.util.Date;

public class DashboardQuery {

    private static final String queryString = "USER > '0' && USER < 'ZZZZZZZ'";
    private static final String logicName = "DashboardQueryLogic";
    private static final String queryName = "DashboardQueryLogic";
    private static final String columnVisibility = "(PUBLIC)";
    private static final QueryPersistence persistence = QueryPersistence.TRANSIENT;
    private static final String parameters = "limit.fields:PAGE_METRICS=1;reduced.response:true;return.fields:" + DashboardFields.getReturnFields();
    private static final boolean trace = false;
    private static final int pageSize = 10000;
    private static final int pageTimeout = -1;
    private static final Long maxResultsOverride = null;
    private static final String systemFrom = "";

    private DashboardQuery() {}

    @SuppressWarnings("unchecked")
    public static ExtJsResponse<DashboardSummary> createQuery(QueryExecutor queryExecutor, String auths, Date beginDate, Date endDate, Date now)
                    throws ParseException {

        MultivaluedMap<String,String> paramsMap = new MultivaluedMapImpl<>();
        paramsMap.putAll(QueryParametersImpl.paramsToMap(logicName, queryString, queryName, columnVisibility, beginDate, endDate, auths,
                        DateUtils.addDays(now, 1), pageSize, pageTimeout, maxResultsOverride, persistence, systemFrom, parameters, trace));
        return (ExtJsResponse) queryExecutor.createQueryAndNext(logicName, paramsMap);
    }
}
