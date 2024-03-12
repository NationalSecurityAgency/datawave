package datawave.iterators.filter.ageoff;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.util.CompositeTimestamp;

/**
 * Data type age off filter. Traverses through indexed tables
 *
 *
 * and non-indexed tables. Example follows. Note that
 *
 * any data type TTL will follow the same units specified in ttl units
 *
 * <pre>
 * {@code
 *
 * <rules>
 *     <rule>
 *         <filterClass>datawave.iterators.filter.ageoff.DataTypeAgeOffFilter</filterClass>
 *         <ttl units="d">720</ttl>
 *         <datatypes>foo,bar</datatypes>
 *         <bar.ttl>44</bar.ttl>
 *     </rule>
 * </rules>
 * }
 * </pre>
 */
public class DataTypeAgeOffFilter extends AppliedRule {

    /**
     * Null byte
     */
    private static final int NULL = 0x00;
    /**
     * Document column
     */
    private static final Text DOCUMENT_COLUMN = new Text("d");
    private static final byte[] DOCUMENT_COLUMN_BYTES = DOCUMENT_COLUMN.getBytes();
    /**
     * Term frequency column.
     */
    private static final Text TF_COLUMN = new Text("tf");
    private static final byte[] TF_COLUMN_BYTES = TF_COLUMN.getBytes();
    /**
     * Fi column
     */
    private static final Text FI_COLUMN = new Text("fi");
    /**
     * Fi column bytes.
     */
    private static final byte[] FI_COLUMN_BYTES = FI_COLUMN.getBytes();

    /**
     * Minimum shard length
     */
    private static final int SHARD_ID_LENGTH_MIN = 10;

    /**
     * Logger
     */
    private static final Logger log = Logger.getLogger(DataTypeAgeOffFilter.class);

    /**
     * Determine whether or not the rules are applied
     */
    protected boolean ruleApplied = false;
    /**
     * Is against the index table.
     */
    protected boolean isIndextable;

    /**
     * Data type cut off times
     */
    protected Map<ByteSequence,Long> dataTypeTimes = null;

    /**
     * When set, will only accept keys at scan-level for matching timestamps
     */
    protected Map<ByteSequence,Long> dataTypeScanTimes = null;

    /**
     * Required by the {@code FilterRule} interface. This method returns a {@code boolean} value indicating whether or not to allow the {@code (Key, Value)}
     * pair through the rule. A value of {@code true} indicates that the pair should be passed onward through the {@code Iterator} stack, and {@code false}
     * indicates that the {@code (Key, Value)} pair should not be passed on.
     *
     * <p>
     * If the value provided in the paramter {@code k} does not match the REGEX pattern specified in this filter's configuration options, then a value of
     * {@code true} is returned.
     *
     * @param k
     *            {@code Key} object containing the row, column family, and column qualifier.
     * @param v
     *            {@code Value} object containing the value corresponding to the {@code Key: k}
     * @return {@code boolean} value indicating whether or not to allow the {@code Key, Value} through the {@code Filter}.
     */
    @Override
    public boolean accept(AgeOffPeriod period, Key k, Value v) {

        ruleApplied = false;
        // get the column qualifier, so that we can use it throughout
        // ASSUMES THAT THE KEY STARTS WITH A CORRECTLY SIZED BYTE ARRAY
        final byte[] cq = k.getColumnQualifierData().getBackingArray();

        int cqLength = cq.length;

        int nullIndex = -1;

        ByteSequence dataType = null;

        /**
         * Supports the shard and index table. There should not be a failure, however if either one is used on the incorrect table
         */
        if (isIndextable) {
            for (int i = SHARD_ID_LENGTH_MIN; i < cqLength; i++) {
                if (cq[i] == NULL) {
                    nullIndex = i + 1;
                    break;
                }
            }

            if (nullIndex > 0)
                dataType = new ArrayByteSequence(cq, nullIndex, (cqLength - nullIndex));

        } else {
            // shard table

            // ASSUMES THAT THE KEY STARTS WITH A CORRECTLY SIZED BYTE ARRAY
            byte[] cf = k.getColumnFamilyData().getBackingArray();

            byte[] column = null;
            if (cf.length >= 3 && cf[0] == FI_COLUMN_BYTES[0] && cf[1] == FI_COLUMN_BYTES[1] && cf[2] == NULL) {
                column = FI_COLUMN_BYTES;
            } else if (cf.length == 2 && cf[0] == TF_COLUMN_BYTES[0]) {
                // no need to check second character as we cannot have a datatype of 't' with an empty UID
                column = TF_COLUMN_BYTES;
            } else if (cf.length == 1 && cf[0] == DOCUMENT_COLUMN_BYTES[0]) {
                column = DOCUMENT_COLUMN_BYTES;
            }

            // if a document or tf column family, then we parse the shard entry differently
            if (column == DOCUMENT_COLUMN_BYTES || column == TF_COLUMN_BYTES) {

                // don't need to check the last byte as we expect more than one null if formatted correctly
                for (int i = 0; i < cqLength - 1; i++) {
                    if (cq[i] == NULL) {
                        nullIndex = i;
                        break;
                    }
                }

                // the data type is the first part of this entry.
                if (nullIndex > 0) {
                    dataType = new ArrayByteSequence(cq, 0, nullIndex);
                }

            } else if (column == FI_COLUMN_BYTES) {

                int uidIndex = -1;
                for (int i = cqLength - 1; i >= 0; i--) {
                    if (cq[i] == NULL) {
                        if (uidIndex == -1)
                            uidIndex = i;
                        else {
                            nullIndex = i + 1;
                        }
                        if (uidIndex > 0 && nullIndex > 0)
                            break;
                    }
                }

                if (uidIndex > 0 && nullIndex > 0)
                    dataType = new ArrayByteSequence(cq, nullIndex, (uidIndex - nullIndex));

            } else {
                int cfLength = cf.length;
                for (int i = 0; i < cfLength; i++) {
                    if (cf[i] == NULL) {
                        nullIndex = i;
                        break;
                    }
                }
                // data column
                if (nullIndex > 0) {
                    dataType = new ArrayByteSequence(cf, 0, nullIndex);
                }

            }
        }

        long defaultCutoffTime = (period.getTtl() >= 0) ? period.getCutOffMilliseconds() : -1;
        Long dataTypeCutoff = (dataTypeTimes.containsKey(dataType)) ? dataTypeTimes.get(dataType) : null;
        boolean accept = true;

        if (dataTypeCutoff == null) {
            if (defaultCutoffTime >= 0) {
                ruleApplied = true;
                accept = CompositeTimestamp.getAgeOffDate(k.getTimestamp()) > defaultCutoffTime;
            }
        } else {
            ruleApplied = true;
            accept = CompositeTimestamp.getAgeOffDate(k.getTimestamp()) > dataTypeCutoff;
        }
        // after age-off is applied check, if we are accepting this KeyValue and this is a Scan on a dataType which only accepts on timestamp
        // only continue to accept the KeyValue if the timestamp for the dataType matches what is configured
        if (accept && iterEnv.getIteratorScope() == IteratorUtil.IteratorScope.scan && dataTypeScanTimes.containsKey(dataType)) {
            final long timestamp = dataTypeScanTimes.get(dataType);
            accept = timestamp == k.getTimestamp();
        }
        return accept;
    }

    /**
     * Required by the {@code FilterRule} interface. Used to initialize the the {@code FilterRule} implementation
     *
     * @param options
     *            {@code Map} object containing the TTL, TTL_UNITS, and MATCHPATTERN for the filter rule.
     * @see datawave.iterators.filter.AgeOffConfigParams
     */
    public void init(FilterOptions options) {
        init(options, null);
    }

    /**
     * Required by the {@code FilterRule} interface. Used to initialize the the {@code FilterRule} implementation
     *
     * @param options
     *            {@code Map} object containing the TTL, TTL_UNITS, and MATCHPATTERN for the filter rule.
     * @param iterEnv
     *            iterator environment
     * @see datawave.iterators.filter.AgeOffConfigParams
     */
    public void init(FilterOptions options, IteratorEnvironment iterEnv) {
        String scanStartStr = options.getOption(AgeOffConfigParams.SCAN_START_TIMESTAMP);
        long scanStart = scanStartStr == null ? System.currentTimeMillis() : Long.parseLong(scanStartStr);
        this.init(options, scanStart, iterEnv);
    }

    protected void init(FilterOptions options, final long scanStart, IteratorEnvironment iterEnv) {
        super.init(options, iterEnv);
        if (options == null) {
            throw new IllegalArgumentException("FilterOptions can not be null");
        }
        String ttlUnits = options.getTTLUnits();

        Set<ByteSequence> dataTypes = Sets.newHashSet();

        String dataTypeOption = options.getOption("datatypes");
        if (null != dataTypeOption) {
            String[] dataTypeArray = dataTypeOption.split(",");
            for (String dt : dataTypeArray)
                dataTypes.add(new ArrayByteSequence(dt.trim().getBytes()));
        }

        isIndextable = false;
        if (options.getOption(AgeOffConfigParams.IS_INDEX_TABLE) == null) {
            if (iterEnv != null && iterEnv.getConfig() != null) {
                isIndextable = Boolean.parseBoolean(iterEnv.getConfig().get("table.custom." + AgeOffConfigParams.IS_INDEX_TABLE));
            }
        } else { // legacy
            isIndextable = Boolean.valueOf(options.getOption(AgeOffConfigParams.IS_INDEX_TABLE));
        }

        long ttlUnitsFactor = 1L; // default to "days" as the unit.

        if (ttlUnits != null) {
            ttlUnitsFactor = AgeOffPeriod.getTtlUnitsFactor(options.getTTLUnits());

            dataTypeTimes = new HashMap<>();
            dataTypeScanTimes = new HashMap<>();

            long myCutOffDateMillis = 0;

            for (ByteSequence dataType : dataTypes) {
                String optionTTL = options.getOption(dataType + "." + AgeOffConfigParams.TTL);
                if (null != optionTTL) {
                    myCutOffDateMillis = scanStart - ((Long.parseLong(optionTTL)) * ttlUnitsFactor);
                    dataTypeTimes.put(dataType, myCutOffDateMillis);
                } else {
                    dataTypeTimes.put(dataType, options.getAgeOffPeriod().getCutOffMilliseconds());
                }

                final String dataTypeHasScanTime = options.getOption(dataType + ".hasScanTime");
                if (Boolean.parseBoolean(dataTypeHasScanTime)) {
                    if (iterEnv != null) {
                        final String scanTime = iterEnv.getConfig().get("table.custom.timestamp.current." + dataType);
                        try {
                            dataTypeScanTimes.put(dataType, Long.parseLong(scanTime, 10));
                        } catch (final NumberFormatException e) {
                            throw new NumberFormatException(dataType + " marked as hasScanTime but corresponding table.custom.timestamp.current." + dataType
                                            + " is invalid: " + scanTime);
                        }
                    } else {
                        throw new NullPointerException("IteratorEnvironment is null");
                    }
                }
            }

        }
    }

    @Override
    public boolean isFilterRuleApplied() {
        return ruleApplied;
    }

}
