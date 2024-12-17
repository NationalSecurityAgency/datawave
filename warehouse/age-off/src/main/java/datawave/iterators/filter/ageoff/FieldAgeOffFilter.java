package datawave.iterators.filter.ageoff;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

import com.google.common.collect.Sets;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.filter.ColumnVisibilityOrFilter;
import datawave.util.CompositeTimestamp;

/**
 * Field age off filter. Traverses through indexed tables and non-indexed tables. Example follows. Note that any field TTL will follow the same units specified
 * in ttl units
 *
 * <pre>
 * {@code
 *
 * <rules>
 *     <rule>
 *         <filterClass>datawave.iterators.filter.ageoff.FieldAgeOffFilter</filterClass>
 *         <ttl units="d">720</ttl>
 *         <datatypes>fieldA,fieldB</datatypes>
 *         <fieldA.ttl>44</fieldA.ttl>
 *     </rule>
 * </rules>
 * }
 * </pre>
 */
public class FieldAgeOffFilter extends AppliedRule {

    protected enum FieldExclusionType {
        EVENT
    }

    public static final String OPTION_PREFIX = "field.";
    private final ColumnVisibilityOrFilter cvOrFilter = new ColumnVisibilityOrFilter();
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
    /**
     * tf column bytes.
     */
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
    private static final Logger log = Logger.getLogger(FieldAgeOffFilter.class);

    /**
     * Determine whether the rules are applied
     */
    protected boolean ruleApplied = false;

    /**
     * Is against the index table.
     */
    protected boolean isIndextable;

    /**
     * Data type cut off times
     */
    protected Map<ByteSequence,Long> fieldTimes = null;

    /**
     * Exclude data from age-off
     */
    protected Set<FieldExclusionType> fieldExcludeOptions = null;

    /**
     * Required by the {@code FilterRule} interface. This method returns a {@code boolean} value indicating whether to allow the {@code (Key, Value)} pair
     * through the rule. A value of {@code true} indicates that he pair should be passed onward through the {@code Iterator} stack, and {@code false} indicates
     * that the {@code (Key, Value)} pair should not be passed on.
     *
     * <p>
     * If the value provided in the parameter {@code k} does not match the REGEX pattern specified in this filter's configuration options, then a value of
     * {@code true} is returned.
     *
     * @param k
     *            {@code Key} object containing the row, column family, and column qualifier.
     * @param v
     *            {@code Value} object containing the value corresponding to the {@code Key: k}
     * @return {@code boolean} value indicating whether to allow the {@code Key, Value} through the {@code Filter}.
     */
    @Override
    public boolean accept(AgeOffPeriod period, Key k, Value v) {

        ruleApplied = false;

        // get the column qualifier, so that we can use it throughout
        final byte[] cq = k.getColumnQualifierData().getBackingArray();

        ByteSequence field = null;
        FieldExclusionType candidateExclusionType = null;

        // Supports the shard and index table. There should not be a failure, however if either one is used on the incorrect table
        if (isIndextable) {
            field = k.getColumnFamilyData();

        } else {
            // shard table

            final byte[] cf = k.getColumnFamilyData().getBackingArray();

            byte[] column = null;
            if (cf.length >= 3 && cf[0] == FI_COLUMN_BYTES[0] && cf[1] == FI_COLUMN_BYTES[1] && cf[2] == NULL) {
                column = FI_COLUMN_BYTES;
            } else if (cf.length == 2 && cf[0] == TF_COLUMN_BYTES[0]) {
                // no need to check second character as we cannot have a datatype of 't' with an empty UID
                column = TF_COLUMN_BYTES;
            } else if (cf.length == 1 && cf[0] == DOCUMENT_COLUMN_BYTES[0]) {
                // if the document column family is encountered, do not attempt to filter its field
                return true;
            }

            if (column == TF_COLUMN_BYTES) {
                // CASE 1
                // The field type is the last fourth part of this entry cq. Use a substring from the last null character to the end of the colQual
                int nullIndex = -1;
                for (int i = cq.length - 1; i >= 0; i--) {
                    if (cq[i] == NULL) {
                        nullIndex = i;
                        break;
                    }
                }
                if (nullIndex > 0) {
                    int start = nullIndex + 1;
                    int length = cq.length - start;
                    field = new ArrayByteSequence(cq, start, length);
                }

            } else if (column == FI_COLUMN_BYTES) {

                // CASE 2
                // For the fi, grab the rest of the string after fi\0
                int start = FI_COLUMN_BYTES.length + 1;
                int length = cf.length - start;
                field = new ArrayByteSequence(cf, start, length);

            } else {
                // CASE 3
                // For the data, find the first null byte or '.' in the colQual, then grab the start of the string to this point
                candidateExclusionType = FieldExclusionType.EVENT;
                int length = -1;
                for (int i = 0; i < cq.length; i++) {
                    if (cq[i] == '.' || cq[i] == NULL) {
                        length = i;
                        break;
                    }
                }

                // event fields may have instance notations using periods
                // the field needs to be truncated to either the null or the first dot.
                if (length > 0) {
                    field = new ArrayByteSequence(cq, 0, length);
                }
            }
        }

        // check to see if the field is excluded based on type
        // if so, pass through the filter
        if ((candidateExclusionType != null && fieldExcludeOptions.contains(candidateExclusionType))) {
            return true;
        }

        Long dataTypeCutoff = fieldTimes.get(field);

        // evaluating field first then the ColumnVisibilityOrFilter based on performance test results
        if (dataTypeCutoff == null || !cvOrFilter.hasToken(k, v, this.cvOrFilter.getPatternBytes())) {
            return true;
        }

        ruleApplied = true;
        return CompositeTimestamp.getAgeOffDate(k.getTimestamp()) > dataTypeCutoff;
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
     *
     * @see datawave.iterators.filter.AgeOffConfigParams
     */
    public void init(FilterOptions options, IteratorEnvironment iterEnv) {
        if (options == null) {
            throw new IllegalArgumentException("FilterOptions can not be null");
        }
        String scanStartStr = options.getOption(AgeOffConfigParams.SCAN_START_TIMESTAMP);
        long scanStart = scanStartStr == null ? System.currentTimeMillis() : Long.parseLong(scanStartStr);
        this.init(options, scanStart, iterEnv);
    }

    protected void init(FilterOptions options, final long startScan, IteratorEnvironment iterEnv) {
        if (options == null) {
            throw new IllegalArgumentException("ttl must be set for a FilterRule implementation");
        }
        super.init(options, iterEnv);
        this.cvOrFilter.init(options, iterEnv);
        String ttlUnits = options.getTTLUnits();

        Set<ByteSequence> fields = Sets.newHashSet();

        String fieldsTypeOption = options.getOption("fields");
        if (null != fieldsTypeOption) {
            String[] fieldsArray = fieldsTypeOption.split(",");
            for (String dt : fieldsArray)
                fields.add(new ArrayByteSequence(dt.trim().getBytes()));
        }

        for (String optionKey : options.options.keySet()) {
            if (optionKey.startsWith(OPTION_PREFIX)) {
                String anotherField = optionKey.substring(OPTION_PREFIX.length(), optionKey.indexOf(".", OPTION_PREFIX.length() + 1));
                fields.add(new ArrayByteSequence(anotherField.trim().getBytes()));
            }
        }

        isIndextable = false;
        if (options.getOption(AgeOffConfigParams.IS_INDEX_TABLE) == null) {
            if (iterEnv != null && iterEnv.getConfig() != null) {
                isIndextable = Boolean.parseBoolean(iterEnv.getConfig().get("table.custom." + AgeOffConfigParams.IS_INDEX_TABLE));
            }
        } else { // legacy
            isIndextable = Boolean.parseBoolean(options.getOption(AgeOffConfigParams.IS_INDEX_TABLE));
        }

        fieldExcludeOptions = new HashSet<>();
        String excludeSchemaOption = options.getOption(AgeOffConfigParams.EXCLUDE_DATA);
        if (excludeSchemaOption != null) {
            for (String schemaTypeText : excludeSchemaOption.split(",")) {
                FieldExclusionType schemaType = Enum.valueOf(FieldExclusionType.class, schemaTypeText.toUpperCase());
                fieldExcludeOptions.add(schemaType);
            }
        }

        long defaultUnitsFactor = 1L; // default to "days" as the unit.

        if (ttlUnits != null) {
            defaultUnitsFactor = options.getAgeOffPeriod().getTtlUnitsFactor();

            fieldTimes = new HashMap<>();

            long myCutOffDateMillis = 0;

            for (ByteSequence fieldName : fields) {
                String optionTTL = options.getOption(OPTION_PREFIX + fieldName + "." + AgeOffConfigParams.TTL);
                if (null == optionTTL) {
                    optionTTL = options.getOption(fieldName + "." + AgeOffConfigParams.TTL);
                }

                String optionTTLUnits = options.getOption(OPTION_PREFIX + fieldName + "." + AgeOffConfigParams.TTL_UNITS);
                if (null == optionTTLUnits) {
                    optionTTLUnits = options.getOption(fieldName + "." + AgeOffConfigParams.TTL_UNITS);
                }
                long optionTTLUnitsFactor = (null != optionTTLUnits ? AgeOffPeriod.getTtlUnitsFactor(optionTTLUnits) : defaultUnitsFactor);

                if (null != optionTTL) {
                    myCutOffDateMillis = startScan - ((Long.parseLong(optionTTL)) * optionTTLUnitsFactor);
                    fieldTimes.put(fieldName, myCutOffDateMillis);
                } else {
                    fieldTimes.put(fieldName, options.getAgeOffPeriod(startScan).getCutOffMilliseconds());
                }
            }
        }
    }

    @Override
    public boolean isFilterRuleApplied() {
        return ruleApplied;
    }
}
