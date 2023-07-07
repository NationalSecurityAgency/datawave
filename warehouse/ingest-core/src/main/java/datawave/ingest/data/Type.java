package datawave.ingest.data;

import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.RecordReader;

import datawave.ingest.data.config.DataTypeHelper;
import datawave.ingest.data.config.ingest.IngestHelperInterface;

public class Type implements Comparable<Type> {
    // This is the name of the type which is used to match types, pull appropriate configuration, determine data handlers
    private String name = null, outputName = null;
    private Class<? extends IngestHelperInterface> helperClass;
    private Class<? extends RecordReader<?,?>> readerClass;
    private String[] defaultDataTypeHandlers;
    private String[] defaultDataTypeFilters;
    private int filterPriority;

    private static final Map<Type,IngestHelperInterface> helpers = new HashMap<>();

    public Type(String name, Class<? extends IngestHelperInterface> helperClass, Class<? extends RecordReader<?,?>> readerClass,
                    String[] defaultDataTypeHandlers, int filterPriority, String[] defaultDataTypeFilters) {
        this(name, name, helperClass, readerClass, defaultDataTypeHandlers, filterPriority, defaultDataTypeFilters);
    }

    public Type(String name, String outputName, Class<? extends IngestHelperInterface> helperClass, Class<? extends RecordReader<?,?>> readerClass,
                    String[] defaultDataTypeHandlers, int filterPriority, String[] defaultDataTypeFilters) {
        this.name = name;
        this.outputName = outputName;
        this.helperClass = helperClass;
        this.readerClass = readerClass;
        this.defaultDataTypeHandlers = defaultDataTypeHandlers;
        this.defaultDataTypeFilters = defaultDataTypeFilters;
        this.filterPriority = filterPriority;
    }

    /**
     * This will return the name of the type
     *
     * @return the name of the type
     */
    public String typeName() {
        return this.name;
    }

    /**
     * This will return the name to be used in accumulo Keys which is everything before the first '.' in the type name. For example a type of "mytype.csv" will
     * return "mytype" as its output name.
     *
     * @return The name to be used in the accumulo Keys
     */
    public String outputName() {
        return this.outputName;
    }

    public String[] getDefaultDataTypeHandlers() {
        return defaultDataTypeHandlers;
    }

    public String[] getDefaultDataTypeFilters() {
        return defaultDataTypeFilters;
    }

    public int getFilterPriority() {
        return filterPriority;
    }

    public Class<? extends IngestHelperInterface> getHelperClass() {
        return helperClass;
    }

    public Class<? extends RecordReader<?,?>> getReaderClass() {
        return readerClass;
    }

    public IngestHelperInterface getIngestHelper(Configuration conf) {
        if (!helpers.containsKey(this) && helperClass != null) {
            synchronized (helpers) {
                if (!helpers.containsKey(this)) {
                    helpers.put(this, newIngestHelper(conf));
                }
            }
        }
        return helpers.get(this);
    }

    public void clearIngestHelper() {
        synchronized (helpers) {
            helpers.remove(this);
        }
    }

    /**
     * @deprecated
     * @param conf
     *            - configuration to set
     * @return helper interface
     */
    public IngestHelperInterface newIngestHelper(Configuration conf) {
        IngestHelperInterface helper = newIngestHelper();
        if (helper != null) {
            conf = new Configuration(conf);
            conf.set(DataTypeHelper.Properties.DATA_NAME, typeName());
            helper.setup(conf);
        }
        return helper;
    }

    /**
     * @deprecated
     * @return helper interface
     */
    public IngestHelperInterface newIngestHelper() {
        IngestHelperInterface helper = null;
        if (helperClass != null) {
            try {
                helper = helperClass.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                // ignore for now
            }
        }
        return helper;
    }

    public RecordReader<?,?> newRecordReader() {
        RecordReader<?,?> reader = null;
        if (readerClass != null) {
            try {
                reader = readerClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                // ignore for now
            }
        }
        return reader;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("<type>");
        builder.append("<typeName>").append(typeName()).append("</typeName>");
        if (!outputName().equals(typeName())) {
            builder.append("<outputName>").append(outputName()).append("</outputName>");
        }
        builder.append("</type>");
        return builder.toString();
    }

    public String toDebugString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("name", this.name);
        tsb.append("outputName", this.outputName);
        tsb.append("helperClass", this.helperClass.getName());
        tsb.append("readerClass", this.readerClass.getName());
        tsb.append("handlers", Arrays.toString(this.defaultDataTypeHandlers));
        tsb.append("filters", Arrays.toString(this.defaultDataTypeFilters));
        tsb.append("filterPriority", this.filterPriority);
        return tsb.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (o == null)
            return false;
        if (o == this)
            return true;
        if (o.getClass() != getClass())
            return false;
        Type other = (Type) o;
        return new EqualsBuilder().append(name, other.name).append(this.outputName, other.outputName).append(this.helperClass, other.helperClass)
                        .append(this.readerClass, other.readerClass).append(this.defaultDataTypeHandlers, other.defaultDataTypeHandlers)
                        .append(this.defaultDataTypeFilters, other.defaultDataTypeFilters).append(this.filterPriority, other.filterPriority).isEquals();

    }

    @Override
    public int compareTo(Type o) {
        return new CompareToBuilder().append(name, o.name).append(outputName, o.outputName).append(getName(this.helperClass), getName(o.helperClass))
                        .append(getName(this.readerClass), getName(o.readerClass)).append(this.defaultDataTypeHandlers, o.defaultDataTypeHandlers)
                        .append(this.defaultDataTypeFilters, o.defaultDataTypeFilters).append(this.filterPriority, o.filterPriority).toComparison();
    }

    private String getName(@SuppressWarnings("rawtypes") Class c) {
        return (c == null ? null : c.getName());
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(this.name).append(this.outputName).append(this.helperClass).append(this.readerClass)
                        .append(this.defaultDataTypeHandlers).append(this.defaultDataTypeFilters).append(this.filterPriority).toHashCode();
    }
}
