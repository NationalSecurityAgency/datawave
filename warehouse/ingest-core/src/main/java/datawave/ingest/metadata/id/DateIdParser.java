package datawave.ingest.metadata.id;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.regex.Matcher;

import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;

public class DateIdParser extends MetadataIdParser {

    private SimpleDateFormat format = null;
    private String value = null;

    public DateIdParser(String pattern, String dateFormat) {
        super(pattern);
        this.format = new SimpleDateFormat(dateFormat);
    }

    public DateIdParser(String pattern, String dateFormat, String value) {
        this(pattern, dateFormat);
        this.value = value;
    }

    @Override
    public void addMetadata(RawRecordContainer event, Multimap<String,String> metadata, String key) throws ParseException {
        Matcher matcher = getMatcher(key);
        if (matcher.matches()) {
            String date = (value == null ? matcher.group(1) : value);
            long time = -1;
            synchronized (format) {
                time = format.parse(date).getTime();
            }
            if (event != null) {
                event.setDate(time);
            }
        }
    }
}
