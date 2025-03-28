package datawave.data.normalizer;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class DateNormalizer extends AbstractNormalizer<Date> {
    
    private static final long serialVersionUID = -3268331784114135470L;
    private static final Logger log = LoggerFactory.getLogger(DateNormalizer.class);
    public static final String ISO_8601_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";
    
    public static final String[] FORMAT_STRINGS = {"EEE MMM dd HH:mm:ss zzz yyyy", // at the top just because
            "EEE MMM dd HH:mm:ss XXX yyyy", // for ISO 8601
            ISO_8601_FORMAT_STRING, "yyyyMMddHHmmss", "yyyy-MM-dd HH:mm:ssz", "yyyy-MM-dd HH:mm:ss'Z'", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd",
            "yyyy-MM-dd'T'HH'|'mm", "yyyy-MM-dd'T'HH:mm:ss'Z'", "yyyy-MM-dd't'HH:mm:ss'z'", "yyyy-MM-dd'T'HH:mm:ssXXX", "yyyy-MM-dd'T'HH:mm:ss.SSSSSSS"};
    
    private static final ThreadLocal<Map<String,SimpleDateFormat>> formatList = new ThreadLocal<Map<String,SimpleDateFormat>>() {
        protected Map<String,SimpleDateFormat> initialValue() {
            return Maps.newHashMap();
        }
    };
    
    public String normalize(String fieldValue) {
        Date fieldDate = parseToDate(fieldValue);
        return parseToString(fieldDate);
    }
    
    public static SimpleDateFormat getParser(String pattern) {
        SimpleDateFormat parser = formatList.get().get(pattern);
        if (parser == null) {
            parser = new SimpleDateFormat();
            parser.setLenient(true);
            parser.applyPattern(pattern);
            formatList.get().put(pattern, parser);
        }
        return parser;
    }
    
    public static String convertMicroseconds(String str, String pattern) {
        // check for a special case where the incoming string is specifying microseconds instead of milliseconds
        if (pattern.lastIndexOf('S') >= 0) {
            // presuming the milliseconds is the last number in the string
            int endMs = str.length();
            int startMs = -1;
            for (int i = endMs - 1; i >= 0; i--) {
                char c = str.charAt(i);
                if (c >= '0' && c <= '9') {
                    startMs = i;
                } else if (startMs == -1) {
                    endMs = i;
                } else {
                    break;
                }
            }
            // drop any characters after 3 digits
            if (endMs - startMs > 3) {
                str = str.substring(0, startMs + 3) + str.substring(endMs);
            }
        }
        return str;
    }
    
    public static Date parseDate(String str, String pattern) {
        SimpleDateFormat parser = getParser(pattern);
        ParsePosition pos = new ParsePosition(0);
        str = convertMicroseconds(str, pattern);
        Date date = parser.parse(str, pos);
        if (date != null && pos.getIndex() == str.length()) {
            return date;
        }
        return null;
    }
    
    public static Date parseDate(String str, String[] parsePatterns) throws ParseException {
        if (str != null && parsePatterns != null) {
            for (int i = 0; i < parsePatterns.length; i++) {
                Date date = parseDate(str, parsePatterns[i]);
                if (date != null) {
                    return date;
                }
            }
            
            throw new ParseException("Unable to parse the date: " + str, -1);
        } else {
            throw new IllegalArgumentException("Date string nor patterns can be null");
        }
    }
    
    private Date parseToDate(String fieldValue) {
        try {
            Date date = parseDate(fieldValue, FORMAT_STRINGS);
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
    
    private Collection<String> formatAll(Date date) {
        List<String> list = Lists.newArrayList();
        for (String format : FORMAT_STRINGS) {
            DateFormat fs = getParser(format);
            String formatted = fs.format(date);
            if (formatted != null && !formatted.isEmpty()) {
                list.add(formatted);
            }
        }
        return list;
    }
    
    public String parseToString(Date date) {
        return getParser(ISO_8601_FORMAT_STRING).format(date);
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
