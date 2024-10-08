package datawave.webservice.query.dashboard;

import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.time.DateUtils;

import datawave.core.common.extjs.ExtJsResponse;
import datawave.core.query.dashboard.DashboardFields;
import datawave.core.query.dashboard.DashboardSummary;
import datawave.microservice.query.DefaultQueryParameters;
import datawave.microservice.query.QueryPersistence;
import datawave.webservice.query.runner.QueryExecutor;
import datawave.webservice.query.util.MapUtils;

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

        return (ExtJsResponse) queryExecutor.createQueryAndNext(logicName,
                        MapUtils.toMultivaluedMap(DefaultQueryParameters.paramsToMap(logicName, queryString, queryName, columnVisibility, beginDate, endDate,
                                        auths, DateUtils.addDays(now, 1), pageSize, pageTimeout, maxResultsOverride, persistence, systemFrom, parameters,
                                        trace)));
    }
}
