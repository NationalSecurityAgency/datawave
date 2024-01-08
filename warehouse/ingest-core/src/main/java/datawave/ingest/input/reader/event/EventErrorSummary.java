package datawave.ingest.input.reader.event;

import java.io.IOException;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.KeyValue;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl3.JexlBuilder;
import org.apache.commons.jexl3.JexlContext;
import org.apache.commons.jexl3.JexlEngine;
import org.apache.commons.jexl3.JexlScript;
import org.apache.commons.jexl3.internal.Engine;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.mapreduce.handler.error.ErrorDataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.util.StringUtils;
import datawave.util.time.DateHelper;

/**
 * This class holds a summary one event's errors stored in the keyValues table
 *
 *
 *
 */
public class EventErrorSummary implements Cloneable, JexlContext {
    // the table
    protected Text tableName = null;
    // the row
    protected Text row = null;
    protected String datatype;
    protected String uid;
    protected String jobName;

    // the error date
    protected Date errorDate = null;
    // the event uuids
    protected List<String> uuids = new ArrayList<>();
    // the event values
    protected List<Value> events = new ArrayList<>();
    // errors mapped to stack trace or null if n/a
    protected Multimap<String,String> errors = HashMultimap.create();
    // the complete set of key values
    protected Multimap<Text,KeyValue> keyValues = HashMultimap.create();
    // the set of original field names and field values
    protected Multimap<String,String> eventFields = HashMultimap.create();
    // the number of times this event was processed
    protected int processedCount = 0;

    public static final Text EVENT = ErrorDataTypeHandler.EVENT_COLF;
    public static final Text INFO = ErrorDataTypeHandler.INFO_COLF;
    public static final Text FIELD = ErrorDataTypeHandler.FIELD_COLF;
    public static final char ERROR_TYPE_KEY_VALUE_SPLIT_CHAR = ':';

    public static final String NULL_SEP = "\0";
    public static final String FI_DESIGNATOR = "fi";

    public EventErrorSummary(EventErrorSummary summary) {
        this.tableName = new Text(summary.tableName);
        this.row = new Text(summary.row);
        this.datatype = summary.datatype;
        this.uid = summary.uid;
        this.jobName = summary.jobName;
        this.errorDate = summary.errorDate;
        this.uuids.addAll(summary.getUuids());
        this.events.addAll(summary.getEvents());
        this.errors.putAll(summary.getErrors());
        this.keyValues.putAll(summary.getKeyValues());
        this.processedCount = summary.processedCount;
    }

    public EventErrorSummary(Text tableName) {
        this.tableName = tableName;
    }

    public EventErrorSummary() {}

    /**
     * Clear out this class for reuse
     */
    public void clear() {
        // not clearing the table name as that might pass from instance to instance
        this.row = null;
        this.datatype = null;
        this.uid = null;
        this.jobName = null;
        this.errorDate = null;
        this.uuids.clear();
        this.events.clear();
        this.errors.clear();
        this.keyValues.clear();
        this.processedCount = 0;
    }

    public boolean isEmpty() {
        return this.keyValues.isEmpty();
    }

    /**
     * Add a key value to this error summary.
     *
     * @param key
     *            the key
     * @param value
     *            the value
     */
    public void addKeyValue(Key key, Value value) {
        validateRow(key.getRow());
        String[] jobAndTypeAndUid = StringUtils.split(key.getRow().toString(), '\0');
        this.jobName = jobAndTypeAndUid[0];
        this.datatype = jobAndTypeAndUid[1];
        this.uid = jobAndTypeAndUid[2];

        KeyValue keyValue = new KeyValue(new Key(key), value.get());
        this.keyValues.put(new Text(keyValue.getKey().getColumnFamily()), keyValue);
        Text cf = key.getColumnFamily();
        if (cf.equals(EVENT)) {
            this.events.add(keyValue.getValue());

            // drop off the event date (which could be the empty string) to get the list of UUIDs
            String cq = key.getColumnQualifier().toString();
            int index = cq.indexOf('\0');
            this.uuids.addAll(Arrays.asList(index >= 0 ? StringUtils.split(cq.substring(index + 1), '\0') : new String[0]));
        } else if (cf.equals(INFO)) {
            String[] info = StringUtils.split(key.getColumnQualifier().toString(), '\0');
            this.errors.put(info[0], (value.getSize() > 0 ? value.toString() : null));
            try {
                // a little validation
                this.errorDate = DateHelper.parse(info[1]);
            } catch (DateTimeParseException pe) {
                throw new IllegalArgumentException("Failed to parse error info date " + key, pe);
            }
        } else if (cf.equals(FIELD)) {
            // look for the processedCount
            String cq = key.getColumnQualifier().toString();
            if (cq.equals(ErrorDataTypeHandler.PROCESSED_COUNT)) {
                String val = value.toString();
                int index = val.indexOf('\0');
                String pcStr = val.substring(0, index);
                this.processedCount = Integer.parseInt(pcStr);
            }
        }
    }

    protected void validateRow(Text row) {
        if (this.row == null) {
            this.row = row;
        } else {
            if (!this.row.equals(row)) {
                throw new IllegalArgumentException("Expected a matching row: " + this.row + " vs " + row);
            }
        }
    }

    public void setTableName(Text tableName) {
        this.tableName = tableName;
    }

    public Text getTableName() {
        return tableName;
    }

    public Text getRow() {
        return row;
    }

    public String getDatatype() {
        return datatype;
    }

    public String getUid() {
        return uid;
    }

    public String getJobName() {
        return jobName;
    }

    public Date getErrorDate() {
        return errorDate;
    }

    public String getFormattedErrorDate() {
        return DateHelper.format(getErrorDate());
    }

    public List<String> getUuids() {
        return uuids;
    }

    public Collection<Value> getEvents() {
        return events;
    }

    public Multimap<String,String> getErrors() {
        return errors;
    }

    public Collection<KeyValue> getErrorInfoKV() {
        return this.keyValues.get(INFO);
    }

    public Collection<KeyValue> getErrorFields() {
        return this.keyValues.get(FIELD);
    }

    public Multimap<Text,KeyValue> getKeyValues() {
        return keyValues;
    }

    public Multimap<String,String> getEventFields() {
        return eventFields;
    }

    public int getProcessedCount() {
        return processedCount;
    }

    public void setProcessedCount(int processedCount) {
        this.processedCount = processedCount;
    }

    /**
     * Validate whether this event error summary is complete
     */
    public void validate() {
        if (this.tableName == null) {
            throw new IllegalStateException("Expected to have a table name for an event summary");
        }
        if (this.row == null) {
            throw new IllegalStateException("Expected to have at least one row for an event summary");
        }
        if (this.keyValues.size() < 2) {
            throw new IllegalStateException("Expected to have at least two rows for an event summary: " + this.row);
        }
        if (this.events.isEmpty()) {
            throw new IllegalArgumentException("Expected at least one event column for " + this.row);
        }
        if (this.errors.isEmpty()) {
            throw new IllegalArgumentException("Expected at least one error for " + this.row);
        }
    }

    /**
     * Determine whether this event error summary matches a set of criteria
     *
     * @param jobName
     *            the job name
     * @param dataType
     *            the data type
     * @param uid
     *            the uid
     * @param specifiedUUIDs
     *            set of specified UUIDs
     * @param errorType
     *            the error type
     * @param dateRange
     *            the date range
     * @param jexlQuery
     *            the jexl query
     * @param maxProcessCount
     *            the max process count
     * @return true if matches, false otherwise
     */
    public boolean matches(String jobName, String dataType, String uid, Set<String> specifiedUUIDs, String errorType, Date[] dateRange, String jexlQuery,
                    int maxProcessCount) {
        boolean matches = true;

        if (maxProcessCount > 0 && processedCount > maxProcessCount) {
            matches = false;
        } else if (jobName != null && !jobName.equals(this.jobName)) {
            matches = false;
        } else if (dataType != null && !dataType.equals(this.datatype)) {
            matches = false;
        } else if (uid != null && !uid.equals(this.uid)) {
            matches = false;
        } else if ((!specifiedUUIDs.isEmpty()) && !uuidMatchFound(specifiedUUIDs, uuids)) {
            matches = false;
        } else if (errorType != null && !matchesError(errorType)) {
            matches = false;
        } else if (dateRange != null) {
            if (dateRange[0] != null && errorDate.before(dateRange[0])) {
                matches = false;
            } else if (dateRange[1] != null && errorDate.after(dateRange[1])) {
                matches = false;
            }
        }
        if (matches && jexlQuery != null) {
            // Get a JexlEngine initialized with the correct JexlArithmetic for this Document
            JexlEngine engine = new Engine(new JexlBuilder().strict(false));

            // Evaluate the JexlContext against the Script
            JexlScript script = engine.createScript(jexlQuery);

            Object o = script.execute(this);

            // Jexl might return us a null depending on the AST
            if (o != null && Boolean.class.isAssignableFrom(o.getClass())) {
                matches = ((Boolean) o);
            } else if (o != null && Collection.class.isAssignableFrom(o.getClass())) {
                // if the function returns a collection of matches, return true/false
                // based on the number of matches
                matches = (!((Collection<?>) o).isEmpty());
            } else {
                matches = false;
            }
        }

        return matches;
    }

    /**
     * Match the error type against the errors. The errorType is of the form {@code <error> :<error text>} where the error text portion is optional. If only the
     * error part exists, then this returns true if the errors contains error as a key. If both the error and the error text exist, then this returns true if
     * the errors contains error as a key, and one of the values contains the specified error text.
     *
     * @param errorType
     *            the error type string
     * @return true if matches
     */
    public boolean matchesError(String errorType) {
        String error = errorType;
        String valueText = null;
        int split = error.indexOf(ERROR_TYPE_KEY_VALUE_SPLIT_CHAR);
        if (split >= 0) {
            error = errorType.substring(0, split);
            valueText = errorType.substring(split + 1);
        }
        if (!errors.containsKey(error)) {
            return false;
        }
        if (valueText != null) {
            for (String value : errors.get(error)) {
                if (value != null && value.indexOf(valueText) >= 0) {
                    return true;
                }
            }
            return false;
        }
        return true;
    }

    /**
     * Determine if an uuid in the list returned from accumulo exists in the input uuid set.
     *
     * @param reprocessUUIDSet
     *            set of UUIDs
     * @param uuidList
     *            list of uuids to match against the set
     * @return flag if the uuid matched
     */
    protected boolean uuidMatchFound(Set<String> reprocessUUIDSet, List<String> uuidList) {
        for (String uuid : uuidList) {
            if (reprocessUUIDSet.contains(uuid))
                return true;
        }
        return false;
    }

    /**
     * A method used to purge this error summary from the error processing table
     *
     * @param context
     *            the context
     * @param writer
     *            the writer
     * @throws InterruptedException
     *             if the process is interrupted
     * @throws IOException
     *             if there is an issue with accessing the table
     */
    @SuppressWarnings({"rawtypes"})
    public void purge(ContextWriter writer, TaskInputOutputContext context) throws IOException, InterruptedException {
        this.purge(writer, context, null, null);
    }

    /**
     * A method used to purge this error summary from the error processing table
     *
     * @param writer
     *            the writer
     * @param context
     *            the context
     * @param event
     *            the event container
     * @param typeMap
     *            the type map
     * @throws InterruptedException
     *             if the process is interrupted
     * @throws IOException
     *             if there is an issue with accessing the table
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void purge(ContextWriter writer, TaskInputOutputContext context, RawRecordContainer event, Map typeMap) throws IOException, InterruptedException {
        for (KeyValue keyValue : this.keyValues.values()) {
            keyValue.getKey().setDeleted(true);
            BulkIngestKey key = new BulkIngestKey(tableName, keyValue.getKey());
            writer.write(key, keyValue.getValue(), context);
        }
    }

    @Override
    public int hashCode() {
        // the keyValues uniquely represents this object
        return keyValues.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof EventErrorSummary) {
            return keyValues.equals(((EventErrorSummary) obj).keyValues);
        }
        return false;
    }

    @Override
    public Object clone() {
        return new EventErrorSummary(this);
    }

    @Override
    public String toString() {
        ToStringBuilder toString = new ToStringBuilder(this);
        toString.append("Table Name", tableName);
        toString.append("Row", row);
        toString.append("DataType", datatype);
        toString.append("Uid", uid);
        toString.append("JobName", jobName);
        toString.append("Uuids", uuids);
        toString.append("Error Date", errorDate);
        toString.append("Errors", errors);
        toString.append("Processed Count", processedCount);
        return toString.toString();
    }

    /*** The JexlContext implementation: get, set, and has ***/
    private Map<String,Object> context = new HashMap<>();

    @Override
    public Object get(String name) {
        Object value = context.get(name);
        if (value == null) {
            Collection c = eventFields.get(name);
            if (c != null && !c.isEmpty()) {
                if (c.size() == 1) {
                    value = c.iterator().next();
                } else {
                    value = c;
                }
            }
        }
        if (value == null) {
            if (name.equals("jobName")) {
                value = this.jobName;
            } else if (name.equals("datatype")) {
                value = this.datatype;
            } else if (name.equals("uid")) {
                value = this.uid;
            } else if (name.equals("errorDate")) {
                value = this.errorDate;
            } else if (name.equals("uuids")) {
                value = this.uuids;
            } else if (name.equals("errors")) {
                value = this.errors;
            } else if (name.equals("processedCount")) {
                value = this.processedCount;
            }
        }
        return value;
    }

    @Override
    public void set(String name, Object value) {
        context.put(name, value);
    }

    @Override
    public boolean has(String name) {
        return get(name) != null;
    }
}
