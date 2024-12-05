package datawave.core.query.logic.filtered;

import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;

import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.microservice.query.Query;

public class DateRangeFilteredQueryLogic extends FilteredQueryLogic {
    public static final Logger log = Logger.getLogger(DateRangeFilteredQueryLogic.class);

    @Override
    public GenericQueryConfiguration initialize(AccumuloClient connection, Query settings, Set<Authorizations> runtimeQueryAuthorizations) throws Exception {
        if (canRunQuery(settings, runtimeQueryAuthorizations)) {
            // may need to adjust the Query to run within the defined range
            if (getFilter() instanceof QueryLogicFilterByDate) {
                QueryLogicFilterByDate filter = (QueryLogicFilterByDate) getFilter();
                applyFilterDateRange(settings, filter);
            } else {
                log.warn("expected QueryLogicFilterByDate filter, but got: " + getFilter().getClass() + " No date range adjustment will be done");
            }
        }

        return super.initialize(connection, settings, runtimeQueryAuthorizations);
    }

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
}
