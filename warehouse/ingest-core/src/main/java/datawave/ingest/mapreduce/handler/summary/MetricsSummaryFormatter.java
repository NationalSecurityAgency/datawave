package datawave.ingest.mapreduce.handler.summary;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.webservice.common.logging.ThreadConfigurableLogger;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Matcher;

public class MetricsSummaryFormatter {
    
    private static final Logger log = ThreadConfigurableLogger.getLogger(MetricsSummaryFormatter.class);
    
    public static final char SEPARATOR = '\u0000';
    private static final int DEFAULT_CAPACITY = 90;
    
    private Multimap<String,NormalizedContentInterface> eventFields;
    private List<StringBuilder> builders;
    
    private boolean isFirstToken;
    
    public List<Text> format(Iterable<String> fieldNames, Multimap<String,NormalizedContentInterface> eventFields, String prefix) {
        isFirstToken = (prefix == null);
        
        this.eventFields = eventFields;
        resetFormattedFields(prefix);
        
        // extract each summaryField from our current eventFields.
        for (String fieldName : fieldNames) {
            addField(fieldName);
            isFirstToken = false;
        }
        
        return asListofTexts();
    }
    
    private List<Text> asListofTexts() {
        List<Text> result = new ArrayList<>(builders.size());
        for (StringBuilder builder : builders) {
            result.add(new Text(builder.toString()));
        }
        return result;
    }
    
    private void resetFormattedFields(String prefix) {
        if (builders == null) {
            builders = new ArrayList<>();
        } else {
            builders.clear();
        }
        builders.add(new StringBuilder(DEFAULT_CAPACITY).append(prefix != null ? prefix : ""));
    }
    
    private void addField(String fieldName) {
        final Object[] fieldValueArray = eventFields.get(fieldName).toArray();
        
        final int fieldValuesSize = fieldValueArray.length;
        
        if (log.isTraceEnabled()) {
            log.trace("Attempting to extract fieldName: [" + fieldName + "]");
            log.trace("There were " + fieldValuesSize + " values for [" + fieldName + "]");
        }
        
        // handle multiple values for a given field
        if (fieldValuesSize > 1) {
            handleMultipleValuesForField(fieldValueArray);
        } else { // skip unnecessary looping when possible
            for (StringBuilder formattedValue : builders) {
                appendFieldValue(fieldValueArray, formattedValue);
            }
        }
    }
    
    private void handleMultipleValuesForField(Object[] fieldValueArray) {
        ArrayList<StringBuilder> newValues = new ArrayList<>(fieldValueArray.length * builders.size());
        
        for (int i = 0; i < fieldValueArray.length; i++) {
            String fieldValue = ((NormalizedContentInterface) fieldValueArray[i]).getEventFieldValue();
            for (StringBuilder currRow : builders) {
                
                if (log.isTraceEnabled()) {
                    log.trace("Adding value: [" + fieldValue + "] to [" + currRow + "]");
                }
                newValues.add(new StringBuilder(DEFAULT_CAPACITY).append(currRow).append(!isFirstToken ? SEPARATOR : "")
                                .append(fieldValue.replaceAll("\0", "_")));
            }
        }
        builders = newValues;
    }
    
    private void appendFieldValue(Object[] fieldValues, StringBuilder formattedValue) {
        if (!isFirstToken)
            formattedValue.append(SEPARATOR);
        if (fieldValues.length != 0) {
            String fieldValue = ((NormalizedContentInterface) fieldValues[0]).getEventFieldValue();
            if (log.isTraceEnabled()) {
                log.trace("Adding formattedValue: [" + fieldValue + "] to [" + formattedValue + "]");
            }
            formattedValue.append(fieldValue.replaceAll("\0", "_"));
        }
    }
    
    public Iterable<Text> getSummaryValuesRegex(final Iterable<Matcher> summaryMatchers, Multimap<String,NormalizedContentInterface> fields) {
        List<Text> results = new ArrayList<>();
        
        for (Matcher matcher : summaryMatchers) {
            Iterable<Entry<String,NormalizedContentInterface>> matches = getMatches(matcher, fields);
            SortedSet<String> unique = new TreeSet<>();
            for (Entry<String,NormalizedContentInterface> e : matches) {
                unique.add(e.getValue().getEventFieldValue());
            }
            results.add(new Text(StringUtils.join(unique, ";")));
        }
        
        return results;
    }
    
    Iterable<Entry<String,NormalizedContentInterface>> getMatches(final Matcher matcher, Multimap<String,NormalizedContentInterface> fields) {
        return Iterables.filter(fields.entries(), input -> {
            // check to see if entry key matches the supplied matcher
            return matcher.reset(input.getKey()).matches();
        });
        
    }
}
