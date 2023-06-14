package datawave.ingest.data.config;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import datawave.data.hash.UID;
import datawave.data.hash.UIDBuilder;
import datawave.data.hash.UIDConstants;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.util.StringUtils;
import datawave.ingest.input.reader.event.RecordFilter;

import org.apache.hadoop.conf.Configuration;

/**
 * DataTypeHelper that supports overriding of the parent data type name via additional config properties. Allows assignment of an event's data type to be
 * dynamically controlled by a known (configured) field within raw data.
 *
 */
public class DataTypeOverrideHelper extends DataTypeHelperImpl {

    public interface Properties {
        String EVENT_DATA_TYPE_FIELD_NAME = ".data.type.field.name";
        String DATA_TYPE_KEYS = ".event.data.type.keys";
        String DATA_TYPE_VALUES = ".event.data.type.values";
        String FILTER_PROP = "RecordReader.filters";
    }

    protected List<? extends RecordFilter> filters = null;

    protected final Map<String,String> eventDataTypeMap = new HashMap<>();
    protected String eventDataTypeFieldName = null;

    public void setup(Configuration config) throws IllegalArgumentException {
        super.setup(config);

        eventDataTypeFieldName = config.get(this.getType().typeName() + Properties.EVENT_DATA_TYPE_FIELD_NAME);

        if (eventDataTypeFieldName != null) {
            String[] eventDataTypeKeys = config.get(this.getType().typeName() + Properties.DATA_TYPE_KEYS, "").split(",");
            String[] eventDataTypeValues = config.get(this.getType().typeName() + Properties.DATA_TYPE_VALUES, "").split(",");

            if (eventDataTypeKeys.length != eventDataTypeValues.length) {
                throw new IllegalArgumentException("Both " + this.getType().typeName() + Properties.DATA_TYPE_KEYS + " and " + this.getType().typeName()
                                + Properties.DATA_TYPE_VALUES + " must contain the same number of values.");
            }

            for (int i = 0; i < eventDataTypeKeys.length; i++)
                this.eventDataTypeMap.put(eventDataTypeKeys[i].trim(), eventDataTypeValues[i].trim());
        }

        if (config.get(Properties.FILTER_PROP) != null) {
            filters = config.getInstances(Properties.FILTER_PROP, RecordFilter.class);
            for (RecordFilter filter : filters) {
                filter.initialize(config);
            }
        }
    }

    /**
     * Update the data of the specified event based on parameters provided. We retrieve the value from the parameters related to
     * <code>eventDataTypeFieldName</code>. If present, we look up the corresponding data type using the values and {@link #getType(String)}.
     *
     * If there are multiple values in <code>eventDataTypeFieldName</code> and more than one value in this field maps to a data type in the
     * <code>eventDataTypeMap</code> the behavior of this method is that the last value in the field will cause the datatype to be set.
     *
     * Otherwise, no action is performed.
     *
     * This variant is used when processing CSV data.
     *
     * @param event
     *            the event on which to set the datatype
     * @param parameters
     *            the fields to inspect
     */
    public void updateEventDataType(RawRecordContainer event, Map<String,Collection<Object>> parameters) {
        if (eventDataTypeFieldName != null && parameters.containsKey(eventDataTypeFieldName)) {
            Collection<Object> c = parameters.get(eventDataTypeFieldName);
            if (c != null) {
                for (Object o : c) {
                    if (o == null)
                        continue;
                    String value = o.toString();
                    event.setDataType(getType(value));
                }
            }
        }
    }

    /**
     * Update the data of the specified event based on the field name and value provided. If the field name does not match <code>eventDataTypeFieldName</code>
     * no action will be performed. Otherwise if this field name matches <code>eventDataTypeFieldName</code>, we will look up the replacement data type using
     * the field value and {@link #getType(String)}.
     *
     * This variant is used when processing CSV data.
     *
     * @param event
     *            the event on which to set the datatype
     * @param fieldName
     *            the name of the field to inspect
     * @param fieldValue
     *            the value of the field to inspect
     */
    public void updateEventDataType(RawRecordContainer event, String fieldName, String fieldValue) {
        if (eventDataTypeFieldName != null && fieldName.equals(eventDataTypeFieldName)) {
            event.setDataType(getType(fieldValue));
        }
    }

    /**
     * Obtain the new type from the eventDataType map based on the field value. This new type is a type in 'output name' only and will reference the internal
     * type, helper, reader, default data type handlers filter priority and default data type filters of the original type (as returned by {@link #getType()};)
     * associated with this helper. If no corresponding type for the specified value is present in the <code>eventDataTypeMap</code> return the original type.
     *
     * @param fieldValue
     *            the field value
     * @return the type.
     */
    protected Type getType(String fieldValue) {
        Type type = this.getType();
        String newType = eventDataTypeMap.get(fieldValue);
        if (newType == null) {
            return type;
        }

        return new Type(type.typeName(), newType, type.getHelperClass(), type.getReaderClass(), type.getDefaultDataTypeHandlers(), type.getFilterPriority(),
                        type.getDefaultDataTypeFilters());

    }

    /**
     * Create a UID which contains hashes based on the base id and appended attachment info
     *
     * @param id
     *            a record ID
     * @param time
     *            a timestamp
     * @return UID
     */
    public static UID getUid(String id, Date time) {
        return getUid(id, time, null);
    }

    /**
     * Create a UID which contains hashes based on the base id and appended attachment info
     *
     * @param id
     *            a record id
     * @param time
     *            a timestamp
     * @param builder
     *            a UID builder, or null to use the default builder (hash-based)
     * @return UID
     */

    public static UID getUid(String id, Date time, UIDBuilder<UID> builder) {
        builder = (builder != null) ? builder : UID.builder();
        String[] uidParts = getIdComponents(id);
        if (uidParts.length > 1) {
            StringBuilder extra = new StringBuilder();
            extra.append(uidParts[1]);
            for (int i = 2; i < uidParts.length; i++) {
                extra.append(UIDConstants.DEFAULT_SEPARATOR).append(uidParts[i]);
            }
            return builder.newId(uidParts[0].getBytes(), time, extra.toString());
        } else {
            return builder.newId(id.getBytes(), time);
        }
    }

    /**
     * Create string array by splitting id with '-att-'
     *
     * @param id
     *            the id to split
     * @return a string array of the split components
     */
    public static String[] getIdComponents(String id) {

        return StringUtils.split(id, "-att-");
    }

    public boolean filterEvent(RawRecordContainer event) {
        boolean result = true;

        if (null != event && filters != null) {
            for (RecordFilter filter : filters) {
                // run through all the filters and make sure they are accepting the event
                // if any of the filters fails to accept, we can't use the event -> short-circuit and set result to false && look for the next one
                if (!filter.accept(event)) {
                    result = false;
                    break;

                }
            }
        }

        return result;
    }

    public void closeFilters() {
        if (filters != null) {
            for (RecordFilter filter : filters) {
                filter.close();
            }
            this.filters = null;
        }
    }
}
