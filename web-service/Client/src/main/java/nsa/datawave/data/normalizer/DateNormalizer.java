package nsa.datawave.data.normalizer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.apache.commons.lang.time.DateUtils;
import org.apache.log4j.Logger;

import com.google.common.collect.Lists;

public class DateNormalizer extends AbstractNormalizer<Date> {
    
    private static final long serialVersionUID = -3268331784114135470L;
    private static final Logger log = Logger.getLogger(DateNormalizer.class);
    public static final String ISO_8601_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    private static final DateFormat sortableDateFormat = new SimpleDateFormat(ISO_8601_FORMAT_STRING);
    
    public static final String[] FORMAT_STRINGS = {
            "EEE MMM dd HH:mm:ss zzz yyyy", // at the top just because
            ISO_8601_FORMAT_STRING, "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ssz", "yyyy-MM-dd HH:mm:ss'Z'", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd",
            "yyyy-MM-dd'T'HH'|'mm", "yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd't'HH:mm:ss'z'", "yyyy-MM-dd'T'HH:mm:ssXXX"};
    
    public static final List<DateFormat> formatList = Lists.newArrayList();
    static {
        for (String fs : FORMAT_STRINGS) {
            DateFormat format = new SimpleDateFormat(fs);
            format.setTimeZone(TimeZone.getTimeZone("GMT"));
            formatList.add(format);
        }
    }
    
    public String normalize(String fieldValue) {
        Date fieldDate = parseToDate(fieldValue);
        return parseToString(fieldDate);
    }
    
    private Date parseToDate(String fieldValue) {
        try {
            Date date = DateUtils.parseDate(fieldValue, FORMAT_STRINGS);
            if (sanityCheck(date.getTime())) {
                return date;
            }
        } catch (ParseException e) {
            if (log.isTraceEnabled()) {
                log.trace("Failed to normalize value using DateUtils: " + fieldValue);
            }
        }
        
        // see if fieldValue looks like a Long value
        try {
            boolean valid = true;
            int size = fieldValue.length();
            long dateLong = 0;
            for (int i = 0; i < size; i++) {
                char c = fieldValue.charAt(i);
                if (c >= '0' && c <= '9') {
                    dateLong *= 10;
                    dateLong += (c - '0');
                } else {
                    valid = false;
                    break;
                }
            }
            if (valid && sanityCheck(dateLong)) {
                return new Date(dateLong);
            }
        } catch (NumberFormatException e) {
            // well, it's not a long
        }
        
        throw new IllegalArgumentException("Failed to normalize value as a Date: " + fieldValue);
        
    }
    
    private boolean sanityCheck(Long dateLong) {
        // between 1900/01/01 and 2100/12/31
        return -2208970800000L <= dateLong && dateLong < 4133894400000L;
    }
    
    private synchronized Collection<String> formatAll(Date date) {
        List<String> list = Lists.newArrayList();
        for (DateFormat fs : formatList) {
            String formatted = fs.format(date);
            if (formatted != null && formatted.length() > 0) {
                list.add(formatted);
            }
        }
        return list;
    }
    
    public synchronized String parseToString(Date date) {
        return sortableDateFormat.format(date);
    }
    
    /**
     * We cannot support regex against dates
     */
    public String normalizeRegex(String fieldRegex) {
        return fieldRegex;
    }
    
    @Override
    public String normalizeDelegateType(Date delegateIn) {
        return parseToString(delegateIn);
    }
    
    @Override
    public Date denormalize(String in) {
        return parseToDate(in);
    }
    
    @Override
    public Collection<String> expand(String dateString) {
        Date date = parseToDate(dateString);
        if (date != null && this.sanityCheck(date.getTime())) {
            return formatAll(date);
        }
        return Collections.emptyList();
    }
    
}
