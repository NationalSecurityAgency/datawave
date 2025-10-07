package datawave.query.language.functions.jexl;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

public class DateFormatUtil {

    private String defaultTime = "000000";
    private String defaultMillisec = "000";
    private static String formatPattern = "yyyyMMdd HHmmss.SSS";
    private static SimpleDateFormat dateFormat = null;

    private static Logger log = Logger.getLogger(DateFormatUtil.class);

    static {
        dateFormat = new SimpleDateFormat(formatPattern);
        dateFormat.setLenient(false);
    }

    public Date fromString(String s) {
        Date d = null;
        ParseException e = null;
        synchronized (DateFormatUtil.dateFormat) {
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
                    d = DateFormatUtil.dateFormat.parse(str);
                    // if any time value in HHmmss was set either by default or by the user
                    // then we want to include ALL of that second by setting the milliseconds to 999
                    if (DateUtils.getFragmentInMilliseconds(d, Calendar.HOUR_OF_DAY) > 0) {
                        DateUtils.setMilliseconds(d, 999);
                    }
                } catch (ParseException pe) {
                    throw new RuntimeException("Unable to parse date " + str + " with format " + formatPattern, e);
                }
            }
        }

        return d;
    }

}
