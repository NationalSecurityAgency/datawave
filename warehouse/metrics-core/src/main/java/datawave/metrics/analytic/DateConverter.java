package datawave.metrics.analytic;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DateConverter {
    public static final String format = "yyyyMMddHHmm";
    public static final String midnight = "0000";

    public static Date convert(String date) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        try {
            return sdf.parse(date.length() == format.length() ? date : date + midnight);
        } catch (ParseException e) {
            return null;
        }
    }

    public static List<Date> convert(String... dates) {
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        ArrayList<Date> ts = new ArrayList<>(dates.length);
        for (String date : dates) {
            try {
                ts.add(sdf.parse(date.length() == format.length() ? date : date + midnight));
            } catch (ParseException e) {}
        }
        return ts;
    }
}
