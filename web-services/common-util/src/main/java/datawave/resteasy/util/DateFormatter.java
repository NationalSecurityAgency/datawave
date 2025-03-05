package datawave.resteasy.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import javax.ws.rs.ext.ParamConverter;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

import com.google.common.base.Preconditions;

import datawave.annotation.DateFormat;

@ParamConverter.Lazy
// Do conversion of default values when needed, not once at deploy time
public class DateFormatter implements ParamConverter<Date> {

    private Logger log = Logger.getLogger(this.getClass());

    private String defaultTime = "000000";
    private String defaultMillisec = "000";
    private static String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat(formatPattern);

    static {
        dateFormat.setLenient(false);
    }

    public DateFormatter(DateFormat format) {
        if (format != null) {
            defaultTime = format.defaultTime();
            defaultMillisec = format.defaultMillisec();
        }
    }

    @Override
    public Date fromString(String s) {
        Date d;
        synchronized (DateFormatter.dateFormat) {
            String str = s;
            if (str.equals("+24Hours")) {
                d = new Date(new Date().getTime() + 86400000);
                log.debug("Param passed in was '+24Hours', setting value to now + 86400000ms");
            } else {
                if (StringUtils.isNotBlank(this.defaultTime) && !str.contains(" ")) {
                    str = str + " " + this.defaultTime;
                }

                if (StringUtils.isNotBlank(this.defaultMillisec) && !str.contains(".")) {
                    str = str + "." + this.defaultMillisec;
                }

                try {
                    d = DateFormatter.dateFormat.parse(str);
                    // if any time value in HHmmss was set either by default or by the user
                    // then we want to include ALL of that second by setting the milliseconds to 999
                    if (DateUtils.getFragmentInMilliseconds(d, Calendar.HOUR_OF_DAY) > 0) {
                        DateUtils.setMilliseconds(d, 999);
                    }
                } catch (ParseException pe) {
                    throw new RuntimeException("Unable to parse date " + str + " with format " + formatPattern, pe);
                }
            }
        }

        return d;
    }

    @Override
    public String toString(Date value) {
        Preconditions.checkNotNull(value);
        synchronized (dateFormat) {
            return dateFormat.format(value);
        }
    }

    public static String getFormatPattern() {
        return formatPattern;
    }

}
