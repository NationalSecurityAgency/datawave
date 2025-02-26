package datawave.core.query.predicate;

import java.util.Date;
import java.util.function.Predicate;

import datawave.microservice.query.Query;

/**
 * Determine if a Query overlaps a defined Date range. A null startDate or endDate is automatically accepted (treated as unbounded).
 */
public class QueryDateRangePredicate implements Predicate<Query> {
    private Date startDate;
    private Date endDate;

    @Override
    public boolean test(Query query) {
        // only test the start date if set
        if (startDate != null) {
            // does the entire query occur before the start date
            if (startDate.after(query.getEndDate())) {
                return false;
            }
        }
        // only test the end date if set
        if (endDate != null) {
            // does the entire query occur after the end date
            if (endDate.before(query.getBeginDate())) {
                return false;
            }
        }

        // some part of the query overlaps the date range
        return true;
    }

    public Date getStartDate() {
        return startDate;
    }

    public void setStartDate(Date startDate) {
        this.startDate = startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public void setEndDate(Date endDate) {
        this.endDate = endDate;
    }
}
