package datawave.webservice.annotation;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * Configuration for the simple lookup service. Currently only holds a start/end date which are set to begin 19700101 - 30000101 by default. Translates dates to
 * string and vice versa, expected format is yyyyMMdd.
 */
public class LookupUUIDServiceConfig {

    private static final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    Date beginDate;
    Date endDate;

    {
        try {
            beginDate = dateFormat.parse("19700101");
            endDate = dateFormat.parse("30000101");
        } catch (java.text.ParseException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public void setBeginDate(String beginDate) {
        try {
            this.beginDate = dateFormat.parse(beginDate);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public void setEndDate(String endDate) {
        try {
            this.endDate = dateFormat.parse(endDate);
        } catch (ParseException e) {
            throw new IllegalArgumentException(e);
        }
    }

    public String getBeginDate() {
        return dateFormat.format(beginDate);
    }

    public String getEndDate() {
        return dateFormat.format(endDate);
    }

    public Date getBeginAsDate() {
        return beginDate;
    }

    public Date getEndAsDate() {
        return endDate;
    }
}
