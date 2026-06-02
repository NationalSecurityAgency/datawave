package datawave.data.hash;

import static datawave.data.hash.UIDConstants.DEFAULT_SEPARATOR;
import static datawave.data.hash.UIDConstants.MILLISECONDS_PER_DAY;

import java.io.DataInput;
import java.io.IOException;
import java.math.BigInteger;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

import datawave.util.StringUtils;

/**
 * Internal, DATAWAVE-specific, unique identifier. Instead of using a UID based on hash values, however, this class uses a 96-bit ID based on a 52-bit
 * timestamp, 20-bit machine ID, and a 24-bit one-up sequence ID.
 * <p>
 * In order to achieve uniqueness, no two threads should use the same machine ID at the same time anywhere in the cluster. Although there are many ways to
 * manage unique machine IDs, each ID is presumed to have been calculated from an 8-bit node ID, a 6-bit process ID, and a 6-bit thread ID. In other words, each
 * machine ID uniquely identifies a single thread within a particular JVM on a particular host.
 * <p>
 * A {@link SnowflakeUID} cannot be used like a {@link HashUID} to recreate equivalent instances from duplicate data. The trade-off is that it helps contiguous
 * entries compress <i>much</i> better in Accumulo.
 */
public class SnowflakeUID extends UID {

    private static final long serialVersionUID = 1856715886248436235L;
    private static final BigInteger TOO_BIG_BIGINT = BigInteger.ONE.shiftLeft(97);
    private static final String[] EMPTY_STRINGS = {};
    private static final BigInteger NID_MASK = BigInteger.valueOf(255L).shiftLeft(36);
    private static final BigInteger PID_MASK = BigInteger.valueOf(63L).shiftLeft(30);
    private static final BigInteger TID_MASK = BigInteger.valueOf(63L).shiftLeft(24);
    private static final BigInteger MACHINE_MASK = BigInteger.valueOf(1048575L).shiftLeft(24);
    private static final BigInteger TIMESTAMP_MASK = BigInteger.valueOf(4503599627370495L).shiftLeft(44);
    private static final BigInteger SEQUENCE_MASK = BigInteger.valueOf(16777215L);

    /**
     * The default radix (hexadecimal) when outputting as a string value
     */
    public static final int DEFAULT_RADIX = 16;

    /**
     * Max value for the 52-bit timestamp (1st field of the overall Snowflake UID)
     */
    public static final long MAX_TIMESTAMP = 4503599627370495L;

    /**
     * Max value for the 8-bit node ID (1st portion of the 20-bit machine ID field)
     */
    public static final int MAX_NODE_ID = 255;

    /**
     * Max value for the 6-bit process ID (2nd portion of the 20-bit machine ID field)
     */
    public static final int MAX_PROCESS_ID = 63;

    /**
     * Max value for the 6-bit thread ID (3rd portion of the 20-bit machine ID field)
     */
    public static final int MAX_THREAD_ID = 63;

    /**
     * Max value for the 20-bit machine ID (2nd field of the overall Snowflake UID)
     */
    public static final int MAX_MACHINE_ID = 1048575;

    /**
     * Max value for the 24-bit sequence ID (3rd field of the overall Snowflake UID)
     */
    public static final int MAX_SEQUENCE_ID = 16777215;

    private final int radix;
    private BigInteger snowflake;

    // cached toString
    private transient String toString = null;

    /**
     * Empty constructor needed if using readFields
     */
    protected SnowflakeUID() {
        radix = DEFAULT_RADIX;
    }

    /**
     * Constructor used by the {@link SnowflakeUIDBuilder}
     *
     * @param rawId
     *            raw 96-bit numerical value
     * @param radix
     *            used for creating a String representation of this ID, such as 10 for decimal and 16 for hexadecimal (the default)
     * @param extras
     *            additional "stuff", if any, to append to the end of the UID, which can be null
     */
    protected SnowflakeUID(final BigInteger rawId, int radix, final String... extras) {
        super(null, true, extras);
        if ((null != rawId) && (rawId.bitLength() > 96)) {
            throw new IllegalArgumentException("Raw ID cannot exceed 96 bits");
        }
        snowflake = rawId;
        this.radix = radix;
    }

    /**
     * Copy constructor with the ability to append "extra" values to any existing "extra" values
     *
     * @param template
     *            the Snowflake-based UID to copy
     * @param extras
     *            extra values, if any, to tack onto the UID
     */
    protected SnowflakeUID(final SnowflakeUID template, final String... extras) {
        super((null != template) ? template.optionalPrefix : null, true, extras);
        this.snowflake = (null != template) ? template.snowflake : null;
        this.radix = (null != template) ? template.radix : DEFAULT_RADIX;
    }

    /**
     * Returns a builder for creating uninitialized Snowflake UIDs used ONLY for deserialization purposes
     *
     * @return a builder for creating uninitialized Snowflake UIDs used ONLY for deserialization purposes
     */
    @SuppressWarnings({"unchecked", "rawtypes"})
    public static UIDBuilder<UID> builder() {
        return (UIDBuilder) new SnowflakeUIDBuilder();
    }

    /**
     * Creates a Snowflake-based UID builder based on the node, process, and thread responsible for queuing the generation of such UIDs
     *
     * @param nodeId
     *            one-up, 8-bit index value for the staging host responsible for queuing the generation of this UID
     * @param processId
     *            one-up, 6-bit index value for the process responsible for queuing the generation of this UID
     * @param threadId
     *            one-up, 6-bit index value for the process thread responsible for queuing the generation of this UID
     * @return a builder used for creating Snowflake-based UIDs
     */
    protected static SnowflakeUIDBuilder builder(int nodeId, int processId, int threadId) {
        return new SnowflakeUIDBuilder(nodeId, processId, threadId);
    }

    /**
     * Creates a Snowflake-based UID builder based on an initial timestamp and the node, process, and thread responsible for queuing the generation of such UIDs
     *
     * @param timestamp
     *            the initial timestamp to use for generating UIDs
     * @param nodeId
     *            one-up, 8-bit index value for the staging host responsible for queuing the generation of this UID
     * @param processId
     *            one-up, 6-bit index value for the process responsible for queuing the generation of this UID
     * @param threadId
     *            one-up, 6-bit index value for the process thread responsible for queuing the generation of this UID
     * @return a builder used for creating Snowflake-based UIDs
     */
    protected static SnowflakeUIDBuilder builder(long timestamp, int nodeId, int processId, int threadId) {
        return new SnowflakeUIDBuilder(timestamp, nodeId, processId, threadId);
    }

    /**
     * Creates a Snowflake-based UID builder based on an initial timestamp, sequence ID, and the node, process, and thread responsible for queuing the
     * generation of such UIDs
     *
     * @param timestamp
     *            the initial timestamp to use for generating UIDs
     * @param nodeId
     *            one-up, 8-bit index value for the staging host responsible for queuing the generation of this UID
     * @param processId
     *            one-up, 6-bit index value for the process responsible for queuing the generation of this UID
     * @param threadId
     *            one-up, 6-bit index value for the process thread responsible for queuing the generation of this UID
     * @param sequenceId
     *            the initial 24-bit sequence value to use for generating UIDs
     * @return a builder used for creating Snowflake-based UIDs
     */
    protected static SnowflakeUIDBuilder builder(long timestamp, int nodeId, int processId, int threadId, int sequenceId) {
        return new SnowflakeUIDBuilder(timestamp, nodeId, processId, threadId, sequenceId);
    }

    /**
     * Creates a {@link SnowflakeUID} builder based on the specified machine ID, which must be a non-negative integer no greater than 20 bits in length (i.e., 0
     * to 1048575).
     *
     * @param machineId
     *            a non-negative integer no greater than 20 bits in length
     * @return a SnowflakeUID builder
     */
    protected static SnowflakeUIDBuilder builder(int machineId) {
        return new SnowflakeUIDBuilder(machineId);
    }

    /**
     * Creates a Snowflake-based UID builder based on an initial timestamp and a 20-bit unique ID of the machine responsible for queuing the generation of such
     * UIDs
     *
     * @param timestamp
     *            the initial timestamp to use for generating UIDs
     * @param machineId
     *            unique ID of the machine responsible for queuing the generation of such UIDs
     * @return a builder used for creating Snowflake-based UIDs
     */
    protected static SnowflakeUIDBuilder builder(long timestamp, int machineId) {
        return new SnowflakeUIDBuilder(timestamp, machineId);
    }

    /**
     * Creates a Snowflake-based UID builder based on an initial timestamp, sequence ID, and a 20-bit unique ID of the machine responsible for queuing the
     * generation of such UIDs
     *
     * @param timestamp
     *            the initial timestamp to use for generating UIDs
     * @param machineId
     *            unique ID of the machine responsible for queuing the generation of such UIDs
     * @param sequenceId
     *            the initial 24-bit sequence value to use for generating UIDs
     * @return a builder used for creating Snowflake-based UIDs
     */
    protected static SnowflakeUIDBuilder builder(long timestamp, int machineId, int sequenceId) {
        return new SnowflakeUIDBuilder(timestamp, machineId, sequenceId);
    }

    @Override
    public int compareTo(final UID uid) {
        int result;
        if (uid instanceof SnowflakeUID) {
            final SnowflakeUID o = (SnowflakeUID) uid;
            final CompareToBuilder compare = new CompareToBuilder();
            if (null == this.snowflake) {
                compare.append(TOO_BIG_BIGINT, o.snowflake);
            } else if (null == o.snowflake) {
                compare.append(this.snowflake, TOO_BIG_BIGINT);
            } else {
                compare.append(this.snowflake, o.snowflake);
            }
            compare.append(this.extra, o.extra);
            result = compare.toComparison();
        } else if (null != uid) {
            result = toString().compareTo(uid.toString());
        } else {
            result = -1;
        }

        return result;
    }

    @Override
    public boolean equals(final Object other) {
        if (!(other instanceof SnowflakeUID)) {
            return false;
        }

        if (other == this) {
            return true;
        }

        return compareTo((UID) other) == 0;
    }

    @Override
    public int getTime() {
        return (int) (getTimestamp() % MILLISECONDS_PER_DAY);
    }

    @Override
    public String getBaseUid() {
        return (null != snowflake) ? snowflake.toString(radix) : "" + null;
    }

    /**
     * Returns the 20-bit "machine ID" portion of the base UID
     *
     * @return the "machine ID" portion of the base UID
     */
    public int getMachineId() {
        if (null != snowflake) {
            BigInteger fragment = new BigInteger(snowflake.toString(16), 16);
            return fragment.and(MACHINE_MASK).shiftRight(24).intValue();
        }

        return -1;
    }

    /**
     * Returns the 8-bit "node ID" portion (1st field) of the machine ID
     *
     * @return the "node ID" portion of the machine ID
     */
    public int getNodeId() {
        if (null != snowflake) {
            final BigInteger fragment = new BigInteger(snowflake.toString(16), 16);
            return fragment.and(NID_MASK).shiftRight(36).intValue();
        }

        return -1;
    }

    /**
     * Returns the 6-bit "process ID" portion (2nd field) of the machine ID
     *
     * @return the "process ID" portion of the machine ID
     */
    public int getProcessId() {
        if (null != snowflake) {
            BigInteger fragment = new BigInteger(snowflake.toString(16), 16);
            return fragment.and(PID_MASK).shiftRight(30).intValue();
        }

        return -1;
    }

    /**
     * Returns the radix used when writing the underlying BigInteger value to a string
     *
     * @return the radix used when writing the underlying BigInteger value to a string
     */
    public int getRadix() {
        return radix;
    }

    /**
     * Returns the one-up, 24-bit "sequence ID" portion of the base UID
     *
     * @return the one-up, 24-bit "sequence ID" portion of the base UID
     */
    public int getSequenceId() {
        if (null != snowflake) {
            BigInteger fragment = new BigInteger(snowflake.toString(16), 16);
            return fragment.and(SEQUENCE_MASK).intValue();
        }

        return -1;
    }

    @Override
    public String getShardedPortion() {
        return getBaseUid();
    }

    /**
     * Returns the underlying BigInteger value of this UID, which may be null if constructed with the no-arg constructor
     *
     * @return the underlying BigInteger value of this UID
     */
    protected BigInteger getSnowflake() {
        return snowflake;
    }

    /**
     * Returns the 6-bit "thread ID" portion (3rd field) of the machine ID
     *
     * @return the "thread ID" portion of the machine ID
     */
    public int getThreadId() {
        if (null != snowflake) {
            BigInteger fragment = new BigInteger(snowflake.toString(16), 16);
            return fragment.and(TID_MASK).shiftRight(24).intValue();
        }

        return -1;
    }

    /**
     * Returns the 52-bit timestamp portion of the base UID
     *
     * @return the 52-bit timestamp portion of the base UID
     */
    public long getTimestamp() {
        if (null != snowflake) {
            BigInteger fragment = new BigInteger(snowflake.toString(16), 16);
            return fragment.and(TIMESTAMP_MASK).shiftRight(44).longValue();
        }

        return -1;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(19, 37).append(this.optionalPrefix).append(this.snowflake).append(this.extra).toHashCode();
    }

    /**
     * Parses the string representation of the UID using the default radix and an unspecified number of extra parts
     *
     * @param s
     *            string of the UID
     * @return SnowflakeUID
     */
    @SuppressWarnings("unchecked")
    public static SnowflakeUID parse(final String s) {
        return parse(s, -1);
    }

    /**
     * Parses the string representation of the UID using the default radix, but only include up to maxExtraParts of the portion past the base hash
     *
     * @param s
     *            string of the UID
     * @param maxExtraParts
     *            is the number of pieces of the extra portion to include. -1 means all, 0 means none.
     * @return UID
     */
    @SuppressWarnings("unchecked")
    public static SnowflakeUID parse(final String s, int maxExtraParts) {
        return parse(s, DEFAULT_RADIX, maxExtraParts);
    }

    /**
     * Parses the string representation of the UID using the specified radix, but only include up to maxExtraParts of the portion past the base hash
     *
     * @param s
     *            string of the UID
     * @param radix
     *            the desired radix
     * @param maxExtraParts
     *            is the number of pieces of the extra portion to include. -1 means all, 0 means none.
     * @return UID
     */
    public static SnowflakeUID parse(final String s, int radix, int maxExtraParts) {
        final String[] parts = (null != s) ? StringUtils.split(s, DEFAULT_SEPARATOR) : EMPTY_STRINGS;
        if (parts.length < 1) {
            throw new IllegalArgumentException("Not a valid object.");
        }

        final BigInteger baseId = (NULL.equals(parts[0])) ? null : new BigInteger(parts[0], radix);
        if ((parts.length > 1) && (maxExtraParts != 0)) {
            final StringBuilder extra = new StringBuilder();
            int limit;
            if (maxExtraParts < 0) {
                limit = parts.length;
            } else {
                limit = Math.min(maxExtraParts + 1, parts.length);
            }

            for (int i = 1; i < limit; i++) {
                if (i > 1) {
                    extra.append(DEFAULT_SEPARATOR);
                }
                extra.append(parts[i]);
            }

            return new SnowflakeUID(baseId, radix, (extra.length() > 0) ? extra.toString() : null);
        } else {
            return new SnowflakeUID(baseId, radix);
        }
    }

    /**
     * Parses the string representation of the hash, but only the base part
     *
     * @param s
     *            string of the UID
     * @return UID
     */
    @SuppressWarnings("unchecked")
    public static SnowflakeUID parseBase(final String s) {
        return parse(s, 0);
    }

    @Override
    public void readFields(final DataInput in) throws IOException {
        final SnowflakeUID input = SnowflakeUID.parse(in.readUTF());
        this.snowflake = input.snowflake;
        this.extra = input.extra;
    }

    @Override
    public String toString() {
        if (toString == null) {
            toString = toString(radix);
        }
        return toString;
    }

    /**
     * Generate the string representation of the Snowflake-based UID using a different radix from the default
     *
     * @param radix
     *            the desired radix
     * @return string representation of the SnowflakeUID
     */
    public String toString(int radix) {
        final StringBuilder builder = new StringBuilder();
        if (null != snowflake) {
            builder.append(snowflake.toString(radix));
        } else {
            builder.append(NULL);
        }

        final String extra = getExtra();
        if ((null != extra) && !extra.isEmpty()) {
            builder.append(DEFAULT_SEPARATOR);
            builder.append(extra);
        }

        return builder.toString();
    }
}
