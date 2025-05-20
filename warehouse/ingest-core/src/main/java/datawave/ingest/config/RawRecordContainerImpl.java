package datawave.ingest.config;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;

import datawave.data.hash.UID;
import datawave.data.hash.UIDBuilder;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.ConfigurationHelper;
import datawave.ingest.data.config.ingest.IgnorableErrorHelperInterface;
import datawave.ingest.protobuf.RawRecordContainer.Data;
import datawave.marking.MarkingFunctions;
import datawave.util.CompositeTimestamp;

public class RawRecordContainerImpl implements Writable, Configurable, RawRecordContainer {

    /*
     * The list of fatal ingest errors. Datatype specific additions can be added by prefixing with the datatype e.g. mydatatype.ingest.fatal.errors
     */
    public static final String INGEST_ERRORS_LIST = "ingest.fatal.errors";
    public static final String[] DEFAULT_INGEST_ERRORS = new String[] {"EVENT_DATE_MISSING", "UID_ERROR", "UUID_MISSING"};

    /*
     * The list of ignorable error helpers. Datatype specific additions can be added by prefixing with the datatype e.g.
     * mydatatype.ingest.ignorable.error.helpers
     */
    public static final String IGNORABLE_ERROR_HELPERS = "ingest.ignorable.error.helpers";

    public static final String USE_TIME_IN_UID = "ingest.uid.include.time.component";
    public static final boolean USE_TIME_IN_UID_DEFAULT = false;
    public static final Type DEFAULT_USE_TIME_IN_UID_KEY = new Type("DEFAULT_USE_TIME_IN_UID_KEY", null, null, null, 0, null);
    private Map<Type,Boolean> useTimeInUid = new HashMap<>();

    private Configuration conf = null;
    private Multimap<Type,String> fatalErrors = HashMultimap.create();
    private Multimap<Type,IgnorableErrorHelperInterface> ignorableErrorHelpers = HashMultimap.create();

    /**
     * This is the composite date for this event
     */
    private long timestamp = CompositeTimestamp.INVALID_TIMESTAMP;
    private Type dataType = null;
    private UID uid = null;
    private UIDBuilder<UID> uidBuilder;
    private Set<String> errors = new ConcurrentSkipListSet<>();
    private ColumnVisibility visibility = null;
    private String rawFileName = null;
    private long rawFileTimeStamp = 0L;
    private long rawRecordNumber = 0;
    private List<String> ids = new ArrayList<>();
    private byte[] rawData = null;
    private boolean requiresMasking = false;
    private Object auxData = null;
    private Map<String,String> auxMap = null;

    // RawRecordContainer support
    Map<String,String> securityMarkings = null;

    public RawRecordContainerImpl() {
        uidBuilder = UID.builder();
    }

    @Override
    public Map<String,String> getSecurityMarkings() {
        return this.securityMarkings;
    }

    @Override
    public void setSecurityMarkings(Map<String,String> securityMarkings) {
        this.securityMarkings = (securityMarkings == null ? null : new HashMap<>(securityMarkings));
        syncSecurityMarkingsToFields();
    }

    @Override
    public void addSecurityMarking(String domain, String marking) {
        if (null == securityMarkings) {
            securityMarkings = new HashMap<>();
        }
        securityMarkings.put(domain, marking);
        syncSecurityMarkingsToFields();
    }

    @Override
    public boolean hasSecurityMarking(String domain, String marking) {
        return null != securityMarkings && securityMarkings.containsKey(domain) && StringUtils.equals(securityMarkings.get(domain), marking);
    }

    protected void syncSecurityMarkingsToFields() {
        if (securityMarkings != null) {
            setVisibility(securityMarkings.get(MarkingFunctions.Default.COLUMN_VISIBILITY));
        } else {
            setVisibility((String) null);
        }
    }

    protected void syncFieldsToSecurityMarkings() {
        if (visibility != null) {
            if (securityMarkings == null) {
                securityMarkings = new HashMap<>();
            }
            securityMarkings.put(MarkingFunctions.Default.COLUMN_VISIBILITY, new String(visibility.getExpression()));

        } else if (securityMarkings != null) {
            securityMarkings.remove(MarkingFunctions.Default.COLUMN_VISIBILITY);
        }
        if (securityMarkings != null && securityMarkings.isEmpty()) {
            securityMarkings = null;
        }
    }

    @Override
    public UID getId() {
        return uid;
    }

    @Override
    public void setId(UID id) {
        setUid(id);
    }

    @Override
    public void generateId(String uidExtra) {
        this.uid = uidBuilder.newId(rawData, getTimeForUID(), uidExtra);
    }

    public UID getUid() {
        return uid;
    }

    /**
     * This method is used by the "reverse ingest" process to set the UID object correctly. It is to be used by the "reverse ingest" process" only, because in
     * the course of the normal ingest process it is set via generateId.
     *
     * @param uid
     *            {@code UID} object
     */
    public void setUid(UID uid) {
        this.uid = uid;
    }

    @Override
    public Type getDataType() {
        return dataType;
    }

    /**
     * Sets the Type for this raw record container.
     *
     * @param dataType
     *            - datatype to set
     * @see Type
     */
    @Override
    public void setDataType(Type dataType) {
        this.dataType = dataType;
    }

    @Override
    public long getTimestamp() {
        return this.timestamp;
    }

    @Override
    public void setTimestamp(long date) {
        this.timestamp = date;
    }

    @Override
    public Collection<String> getErrors() {
        return Collections.unmodifiableSet(errors);
    }

    @Override
    public void setErrors(Collection<String> errors) {
        this.errors.addAll(errors);
    }

    @Override
    public void addError(String error) {
        errors.add(error);
    }

    @Override
    public void removeError(String error) {
        if (null != errors && errors.contains(error)) {
            errors.remove(error);
        }
    }

    @Override
    public boolean hasError(String error) {
        return errors.contains(error);
    }

    /**
     * Checks the error list for errors that are deemed fatal. See the INGEST_ERRORS_LIST in the configuration for the list of fatal conditions.
     *
     * @return true if any of the fatal errors are encountered, else false.
     */
    @Override
    public boolean fatalError() {
        Set<String> fatalErrors = getFatalErrors();
        for (String e : errors) {
            if (fatalErrors.contains(e)) {
                return true;
            }
        }
        return false;
    }

    /**
     * This is called to determine whether a event with fatal errors is ignorable and subsequently dropped.
     *
     * @return true if any ignorable error helper deems this event ignorable, false otherwise.
     */
    @Override
    public boolean ignorableError() {
        Set<String> fatalErrors = getFatalErrors();
        for (String err : errors) {
            if (fatalErrors.contains(err)) {
                // if one helper deems it ignorable, then this error is ignorable
                boolean ignorable = false;
                for (IgnorableErrorHelperInterface helper : getIgnorableErrorHelpers()) {
                    if (helper.isIgnorableFatalError(this, err)) {
                        ignorable = true;
                    }
                }
                // if no helpers deemed this error ignorable, then this event's fatal errors are not ignorable
                if (!ignorable) {
                    return false;
                }
            }
        }
        // no fatal error was found to be not ignorable, so the final answer is that this event's errors ARE ignorable
        return true;
    }

    public Collection<IgnorableErrorHelperInterface> getIgnorableErrorHelpers() {
        List<IgnorableErrorHelperInterface> helpers = new ArrayList<>();
        helpers.addAll(this.ignorableErrorHelpers.get(null));
        helpers.addAll(this.ignorableErrorHelpers.get(getDataType()));
        return helpers;
    }

    /**
     * This method is used by the "reverse ingest" process to clear the error values that are set by the generateId() method. This is due to the fact that the
     * Event object is not fully populated in "reverse ingest" process.
     */
    @Override
    public void clearErrors() {
        errors.clear();
    }

    @Override
    public Collection<String> getAltIds() {
        return this.ids;
    }

    @Override
    public void setAltIds(Collection<String> altIds) {
        this.ids = Lists.newArrayList(altIds);
    }

    @Override
    public void addAltId(String altId) {
        if (null != this.ids && !this.ids.contains(altId)) {
            this.ids.add(altId);
        } else {
            this.ids = Lists.newArrayList(altId);
        }
    }

    @Override
    public boolean hasAltId(String altId) {
        return null != ids && ids.contains(altId);
    }

    @Override
    public String getRawFileName() {
        return rawFileName;
    }

    /**
     * Stores the name of the raw file that this event came from.
     *
     * @param rawFileName
     *            - name of the raw file
     */
    @Override
    public void setRawFileName(String rawFileName) {
        this.rawFileName = rawFileName;
    }

    /**
     * Get the raw record number
     *
     * @return the raw record number
     */
    @Override
    public long getRawRecordNumber() {
        return rawRecordNumber;
    }

    /**
     * Stores the record number in the raw file that this raw record container came from.
     *
     * @param rawRecordNumber
     *            - the record number to store
     */
    @Override
    public void setRawRecordNumber(long rawRecordNumber) {
        this.rawRecordNumber = rawRecordNumber;
    }

    /**
     *
     * @return timestamp of the raw input file
     */
    @Override
    public long getRawFileTimestamp() {
        return this.rawFileTimeStamp;
    }

    /**
     * Stores the timestamp of the raw input file that this raw record container came from.
     *
     * @param rawRecordTimestamp
     *            - the record timestamp
     */
    @Override
    public void setRawFileTimestamp(long rawRecordTimestamp) {
        this.rawFileTimeStamp = rawRecordTimestamp;
    }

    @Override
    public byte[] getRawData() {
        return rawData;
    }

    @Override
    public void setRawData(byte[] rawData) {
        this.rawData = rawData;
    }

    /**
     * Gets any auxiliary data stored with this raw record container. Note that aux data is not serialized with the raw record container.
     */
    @Override
    public Object getAuxData() {
        return auxData;
    }

    /**
     * Sets auxiliary data for this raw record container. Note that aux data is not serialized with the raw record container.
     */
    @Override
    public void setAuxData(Object auxData) {
        this.auxData = auxData;
    }

    /**
     * Gets any auxiliary properties stored with this raw record container. Note that aux properties are not serialized with the raw record container.
     */
    @Override
    public String getAuxProperty(String prop) {
        return (auxMap == null ? null : auxMap.get(prop));
    }

    /**
     * Sets an auxiliary property for this raw record container. Note that aux properties are not serialized with the raw record container.
     */
    @Override
    public void setAuxProperty(String prop, String value) {
        if (auxMap == null) {
            auxMap = new HashMap<>();
        }
        auxMap.put(prop, value);
    }

    /**
     * @return Copy of this RwaRecordContainerImpl object.
     */
    @Override
    public RawRecordContainerImpl copy() {
        return copyInto(new RawRecordContainerImpl());
    }

    @Override
    public boolean equals(Object other) {
        if (other == null) {
            return false;
        }
        if (other == this) {
            return true;
        }
        if (other.getClass() != getClass()) {
            return false;
        }
        RawRecordContainerImpl e = (RawRecordContainerImpl) other;
        EqualsBuilder equals = new EqualsBuilder();
        equals.append(this.timestamp, e.timestamp);
        equals.append(this.dataType, e.dataType);
        equals.append(this.uid, e.uid);
        equals.append(this.errors, e.errors);
        equals.append(this.visibility, e.visibility);
        equals.append(this.rawFileName, e.rawFileName);
        equals.append(this.rawFileTimeStamp, e.rawFileTimeStamp);
        equals.append(this.rawRecordNumber, e.rawRecordNumber);
        equals.append(this.ids, e.ids);
        equals.append(this.rawData, e.rawData);
        equals.append(this.auxData, e.auxData);
        equals.append(this.auxMap, e.auxMap);
        equals.append(this.securityMarkings, e.securityMarkings);
        return equals.isEquals();
    }

    @Override
    public int hashCode() {
        int result = (int) (timestamp ^ (timestamp >>> 32));
        result = 31 * result + (dataType != null ? dataType.hashCode() : 0);
        result = 31 * result + (uid != null ? uid.hashCode() : 0);
        result = 31 * result + (errors != null ? errors.hashCode() : 0);
        result = 31 * result + (visibility != null ? visibility.hashCode() : 0);
        result = 31 * result + (rawFileName != null ? rawFileName.hashCode() : 0);
        result = 31 * result + (int) (rawFileTimeStamp ^ (rawFileTimeStamp >>> 32));
        result = 31 * result + (int) (rawRecordNumber ^ (rawRecordNumber >>> 32));
        result = 31 * result + (ids != null ? ids.hashCode() : 0);
        result = 31 * result + Arrays.hashCode(rawData);
        result = 31 * result + (auxData != null ? auxData.hashCode() : 0);
        result = 31 * result + (auxMap != null ? auxMap.hashCode() : 0);
        result = 31 * result + (securityMarkings != null ? securityMarkings.hashCode() : 0);
        return result;
    }

    /**
     * Copy this RawRecordConatiner into another RawRecordContainer
     *
     * @param rrci
     *            - the record container to copy
     * @return a copied raw record container
     */
    protected RawRecordContainerImpl copyInto(RawRecordContainerImpl rrci) {
        copyConfiguration(rrci);

        rrci.timestamp = this.timestamp;
        rrci.dataType = this.dataType;
        rrci.uid = this.uid;
        rrci.errors = new ConcurrentSkipListSet<>(this.errors);
        rrci.visibility = this.visibility;
        rrci.rawFileName = this.rawFileName;
        rrci.rawFileTimeStamp = this.rawFileTimeStamp;
        rrci.rawRecordNumber = this.rawRecordNumber;
        rrci.securityMarkings = this.securityMarkings;
        rrci.ids = new ArrayList<>(this.ids);
        rrci.rawData = this.rawData;
        rrci.requiresMasking = this.requiresMasking;
        rrci.auxData = auxData;
        rrci.auxMap = (auxMap == null ? null : new HashMap<>(auxMap));
        return rrci;
    }

    /**
     * Since the configuration has already been loaded, there is no need to re-load it. We can simply copy the built objects
     *
     * @param rrci
     *            - the record container from which to pull a configuration
     */
    private void copyConfiguration(RawRecordContainerImpl rrci) {
        rrci.conf = conf;
        rrci.fatalErrors.putAll(fatalErrors);
        rrci.ignorableErrorHelpers.putAll(ignorableErrorHelpers);
        rrci.useTimeInUid.putAll(useTimeInUid);
    }

    @Override
    /**
     * This will report the number of bytes taken by the RawRecordContainer object when written out. Note that write(DataOutput) or readFields(DataInput) must
     * have been called previously otherwise this will return -1.
     */
    public long getDataOutputSize() {
        return dataOutputSize;
    }

    public Set<String> getFatalErrors() {
        Set<String> localErrors = new HashSet<>();

        localErrors.addAll(this.fatalErrors.get(null));

        localErrors.addAll(this.fatalErrors.get(getDataType()));
        return Collections.unmodifiableSet(localErrors);
    }

    public List<String> getIds() {
        return ids;
    }

    @Override
    public ColumnVisibility getVisibility() {
        return visibility;
    }

    @Override
    public void setVisibility(ColumnVisibility visibility) {
        this.visibility = visibility;
        syncFieldsToSecurityMarkings();
    }

    public void setVisibility(String visibility) {
        setVisibilityNoSync(visibility);
        syncFieldsToSecurityMarkings();
    }

    private void setVisibilityNoSync(String visibility) {
        if (visibility == null) {
            this.visibility = null;
        } else {
            this.visibility = new ColumnVisibility(visibility);
        }
    }

    @Override
    public Date getTimeForUID() {
        boolean useTimeInUid = USE_TIME_IN_UID_DEFAULT;
        if (this.useTimeInUid.containsKey(getDataType())) {
            useTimeInUid = this.useTimeInUid.get(getDataType());
        } else if (this.useTimeInUid.containsKey(DEFAULT_USE_TIME_IN_UID_KEY)) {
            useTimeInUid = this.useTimeInUid.get(DEFAULT_USE_TIME_IN_UID_KEY);
        }
        if (useTimeInUid) {
            return new Date(getDate());
        }
        return null;
    }

    @Override
    public boolean isRequiresMasking() {
        return requiresMasking;
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        // this gets called every time getCurrentValue is called on the EventSequenceRecordReader, so try and avoid
        // reloading configuration every time...
        if (this.conf != conf) {
            this.conf = conf;
            reloadConfiguration();
        }
    }

    public void reloadConfiguration() {
        this.fatalErrors = HashMultimap.create();
        this.ignorableErrorHelpers = HashMultimap.create();
        this.uidBuilder = (null != this.conf) ? UID.builder(this.conf) : UID.builder();

        if (this.conf == null) {
            return;
        }

        String[] errors = this.conf.getStrings(INGEST_ERRORS_LIST, DEFAULT_INGEST_ERRORS);
        if (errors != null) {
            for (String error : errors) {
                this.fatalErrors.put(null, error);
            }
        }

        List<IgnorableErrorHelperInterface> ignorableHelpers = ConfigurationHelper.getInstances(this.conf, IGNORABLE_ERROR_HELPERS,
                        IgnorableErrorHelperInterface.class);
        if (ignorableHelpers != null) {
            for (IgnorableErrorHelperInterface helper : ignorableHelpers) {
                helper.setup(this.conf);
                this.ignorableErrorHelpers.put(null, helper);
            }
        }

        this.useTimeInUid.put(DEFAULT_USE_TIME_IN_UID_KEY, this.conf.getBoolean(USE_TIME_IN_UID, USE_TIME_IN_UID_DEFAULT));

        // now the datatype specific stuff
        TypeRegistry registry = TypeRegistry.getInstance(this.conf);
        for (Map.Entry<String,String> prop : conf) {
            String propName = prop.getKey();
            if (propName.endsWith(INGEST_ERRORS_LIST) && !propName.equals(INGEST_ERRORS_LIST)) {
                String typeName = propName.substring(0, propName.length() - INGEST_ERRORS_LIST.length() - 1);
                if (registry.containsKey(typeName)) {
                    Type dataType = registry.get(typeName);
                    errors = this.conf.getStrings(propName);
                    if (errors != null) {
                        for (String error : errors) {
                            this.fatalErrors.put(dataType, error);
                        }
                    }
                }
            } else if (propName.endsWith(IGNORABLE_ERROR_HELPERS) && !(propName.equals(IGNORABLE_ERROR_HELPERS))) {
                String typeName = propName.substring(0, propName.length() - IGNORABLE_ERROR_HELPERS.length() - 1);
                if (registry.containsKey(typeName)) {
                    Type dataType = registry.get(typeName);
                    ignorableHelpers = ConfigurationHelper.getInstances(this.conf, propName, IgnorableErrorHelperInterface.class);
                    if (ignorableHelpers != null) {
                        for (IgnorableErrorHelperInterface helper : ignorableHelpers) {
                            helper.setup(this.conf);
                            this.ignorableErrorHelpers.put(dataType, helper);
                        }
                    }
                }
            } else if (propName.endsWith(USE_TIME_IN_UID) && !(propName.equals(USE_TIME_IN_UID))) {
                String typeName = propName.substring(0, propName.length() - USE_TIME_IN_UID.length() - 1);
                if (registry.containsKey(typeName)) {
                    Type dataType = registry.get(typeName);
                    useTimeInUid.put(dataType, this.conf.getBoolean(propName, USE_TIME_IN_UID_DEFAULT));
                }
            }
        }

    }

    private long dataOutputSize = -1;

    @Override
    public void write(DataOutput out) throws IOException {
        Data.Builder builder = Data.newBuilder();
        builder.setDate(this.timestamp);
        if (null != this.dataType)
            builder.setDataType(this.dataType.typeName());
        if (null != this.uid)
            builder.setUid(this.uid.toString());
        for (String error : errors)
            builder.addErrors(error);
        if (null != this.visibility)
            builder.setVisibility(ByteString.copyFrom(this.visibility.getExpression()));
        if (null != this.rawFileName)
            builder.setRawFileName(this.rawFileName);
        builder.setRawRecordNumber(this.rawRecordNumber);
        builder.setRawFileTimeStamp(this.rawFileTimeStamp);
        builder.addAllUuids(this.ids);
        if (null != this.rawData)
            builder.setRawData(ByteString.copyFrom(this.rawData));
        builder.setRequiresMasking(this.requiresMasking);

        Data data = builder.build();
        byte[] buf = data.toByteArray();
        out.writeInt(buf.length);
        out.write(buf);
        this.dataOutputSize = buf.length;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        clear();
        int length = in.readInt();
        this.dataOutputSize = length;
        byte[] buf = new byte[length];
        in.readFully(buf);
        Data data = Data.parseFrom(buf);

        this.timestamp = data.getDate();
        if (data.hasDataType())
            try {
                this.dataType = TypeRegistry.getType(data.getDataType());
            } catch (IllegalStateException ise) {
                // Try to initialize the registry and try again.
                // This was put in so that hadoop fs -conf <confFiles> -text <fileName> would work.
                if (null != conf)
                    TypeRegistry.getInstance(conf);
                this.dataType = TypeRegistry.getType(data.getDataType());
            }
        if (data.hasUid())
            this.uid = UID.parse(data.getUid());
        errors = new ConcurrentSkipListSet<>();
        if (0 != data.getErrorsCount()) {
            errors.addAll(data.getErrorsList());
        }
        if (data.hasVisibility() && null != data.getVisibility())
            setVisibility(new ColumnVisibility(data.getVisibility().toByteArray()));
        if (data.hasRawFileName())
            this.rawFileName = data.getRawFileName();
        if (data.hasRawFileTimeStamp())
            this.rawFileTimeStamp = data.getRawFileTimeStamp();
        if (data.hasRawRecordNumber())
            this.rawRecordNumber = data.getRawRecordNumber();
        if (0 != data.getUuidsCount())
            this.ids = new ArrayList<>(data.getUuidsList());
        if (data.hasRawData() && null != data.getRawData())
            this.rawData = data.getRawData().toByteArray();
        this.requiresMasking = data.getRequiresMasking();
    }

    /**
     * Resets state for re-use.
     */
    public void clear() {
        timestamp = CompositeTimestamp.INVALID_TIMESTAMP;
        dataType = null;
        uid = null;
        errors.clear();
        visibility = null;
        securityMarkings = null;
        rawFileName = null;
        rawFileTimeStamp = 0L;
        rawRecordNumber = 0;
        ids.clear();
        rawData = null;
        requiresMasking = false;
        auxData = null;
        auxMap = null;
        dataOutputSize = -1;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString() {
        ToStringBuilder buf = new ToStringBuilder(this);
        buf.append("timestamp", this.timestamp);
        buf.append("dataType", dataType.typeName());
        buf.append("uid", String.valueOf(this.uid));
        buf.append("errors", errors);
        buf.append("visibility", String.valueOf(this.visibility));
        buf.append("securityMarkings", this.securityMarkings);
        buf.append("rawFileName", this.rawFileName);
        buf.append("rawFileTimeStamp", this.rawFileTimeStamp);
        buf.append("rawRecordNumber", this.rawRecordNumber);
        buf.append("IDs", ids);
        buf.append("RawDataLength", this.rawData.length);

        return buf.toString();
    }

}
