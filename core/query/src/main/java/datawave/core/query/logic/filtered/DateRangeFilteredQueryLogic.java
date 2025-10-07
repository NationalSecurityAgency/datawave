package datawave.core.query.logic.filtered;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.Query;

/**
 * A FilteredQueryLogic which can limit the begin and end date of a Query by the configured filter dates when paired with a QueryLogicFilterByDate
 */
public class DateRangeFilteredQueryLogic extends FilteredQueryLogic {
    public static final Logger log = Logger.getLogger(DateRangeFilteredQueryLogic.class);

    public DateRangeFilteredQueryLogic() {}

    public DateRangeFilteredQueryLogic(FilteredQueryLogic other) throws CloneNotSupportedException {
        super(other);
    }

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        if (canRunQuery(settings, runtimeQueryAuthorizations)) {
            // may need to adjust the Query to run within the defined range
            if (getFilter() instanceof QueryLogicFilterByDate) {
                QueryLogicFilterByDate filter = (QueryLogicFilterByDate) getFilter();
                applyFilterDateRange(settings, filter);
            } else {
                log.warn("Not adjusting date range. Filter is not QueryLogicFilterByDate filter: " + getFilter().getClass());
            }
        }

        return super.initialize(connection, settings, runtimeQueryAuthorizations);
    }

    /**
     * Update the Query so that the begin date is no earlier than the filters begin date, and the end date is no later than the filters end date
     *
     * @param settings
     *            the query settings to adjust
     * @param filter
     *            the filter to use to adjust the query settings
     */
    private void applyFilterDateRange(Query settings, QueryLogicFilterByDate filter) {
        boolean modifiedDates = false;
        if (filter.getStartDate() != null && filter.getStartDate().after(settings.getBeginDate())) {
            // adjust the start to match the filter start
            log.info("adjusting query: " + settings.getId() + " start date from: " + settings.getBeginDate() + " to " + filter.getStartDate());
            settings.setBeginDate(filter.getStartDate());
            modifiedDates = true;
        }

        if (filter.getEndDate() != null && filter.getEndDate().before(settings.getEndDate())) {
            // adjust the end date to match the filter end
            log.info("adjusting query: " + settings.getId() + " end date from: " + settings.getEndDate() + " to " + filter.getEndDate());
            settings.setEndDate(filter.getEndDate());
            modifiedDates = true;
        }

        if (modifiedDates) {
            log.info("final date range for query: " + settings.getId() + " " + settings.getBeginDate() + " to " + settings.getEndDate());
        }
    }

    @Override
    public Object clone() throws CloneNotSupportedException {
        return new DateRangeFilteredQueryLogic(this);
    }
}
