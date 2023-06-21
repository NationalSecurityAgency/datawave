package datawave.data.hash;

import static datawave.data.hash.UIDConstants.DEFAULT_SEPARATOR;
import static datawave.data.hash.UIDConstants.TIME_SEPARATOR;

import java.io.DataInput;
import java.io.IOException;
import java.util.Date;

import datawave.util.StringUtils;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.util.hash.Hash;
import org.apache.hadoop.util.hash.MurmurHash;

/**
 * Internal, DATAWAVE-specific, unique identifier. Instead of using a UUID which consumes 128 bits, we are using:
 *
 * originally: two concatenated int values that are results of computing a Murmur hash on the raw bytes using two different seeds. The resulting UID has the
 * form:
 *
 * OptionPrefix.Hash1.Hash2
 *
 * currently: three concatenated int values that are results of computing a Murmur hash on the raw bytes using three different seeds. The resulting UID has the
 * form:
 *
 * Hash0.Hash1.Hash2
 *
 * optionally: Additional data can be tagged onto the end
 *
 * Hash0.Hash1.Hash2.Stuff
 *
 */
public class HashUID extends UID {

    private static final long serialVersionUID = 4016018180334520481L;
    private static final HashUIDBuilder DEFAULT_BUILDER = new HashUIDBuilder();
    private static final int SEED0 = 2011;
    private static final int SEED1 = 650567;
    private static final int SEED2 = 22051009;
    private static Hash hash = MurmurHash.getInstance();

    private int h1 = 0;

    private int h2 = 0;

    private int time = -1;

    // cached toString
    private transient String toString = null;

    /**
     * Empty constructor needed if using readFields
     */
    protected HashUID() {}

    /**
     * Construct a uid using some data to hash
     *
     * @param data
     *            The data to hash.
     * @param time
     *            The time to use, can be null (@see Event.getTimeForUID()).
     */
    protected HashUID(final byte[] data, final Date time) {
        this(data, time, EMPTY_EXTRAS);
    }

    /**
     * Construct a uid using some data to hash and add some extra on the end. Note that only the hashed portion of the UID is used for shard calculations.
     *
     * @param data
     *            The data to hash.
     * @param time
     *            The time to use, can be null (@see Event.getTimeForUID()).
     * @param extras
     *            Extra stuff to append to the end of the UID, can be null.
     */
    protected HashUID(final byte[] data, final Date time, final String... extras) {
        super(null, true, extras);
        hash(data);
        this.time = extractTimeOfDay(time);
    }

    /**
     * Copy constructor with the ability to append "extra" values to any existing "extra" values
     *
     * @param template
     *            the hash-based UID to copy
     * @param extras
     *            extra values, if any, to tack onto the UID
     */
    protected HashUID(final HashUID template, final String... extras) {
        super((null != template) ? template.optionalPrefix : null, true, extras);
        this.h1 = (null != template) ? template.h1 : 0;
        this.h2 = (null != template) ? template.h2 : 0;
        this.time = (null != template) ? template.time : -1;
    }

    private HashUID(final String optionalPrefix, int h1, int h2, int time, final String... extras) {
        super(optionalPrefix, true, extras);
        this.h1 = h1;
        this.h2 = h2;
        this.time = time;
    }

    private HashUID(final String optionalPrefix, int h1, int h2, int time) {
        this(optionalPrefix, h1, h2, time, EMPTY_EXTRAS);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static UIDBuilder<UID> builder() {
        return (UIDBuilder) DEFAULT_BUILDER;
    }

    @Override
    public int compareTo(final UID uid) {
        int result;
        if (uid instanceof HashUID) {
            final HashUID o = (HashUID) uid;
            final CompareToBuilder compare = new CompareToBuilder();
            compare.append(this.optionalPrefix, o.optionalPrefix);
            compare.append(this.h1, o.h1);
            compare.append(this.h2, o.h2);
            compare.append(this.time, o.time);
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
    public boolean equals(Object other) {
        if (!(other instanceof HashUID)) {
            return false;
        }

        if (other == this) {
            return true;
        }

        HashUID o = (HashUID) other;
        EqualsBuilder equals = new EqualsBuilder();
        equals.append(this.optionalPrefix, o.optionalPrefix);
        equals.append(this.h1, o.h1);
        equals.append(this.h2, o.h2);
        equals.append(this.time, o.time);
        equals.append(this.extra, o.extra);

        return equals.isEquals();
    }

    /**
     * Get the base portion of the uid (sans the extra part)
     *
     * @return the base uid
     */
    public String getBaseUid() {
        StringBuilder buf = new StringBuilder();
        buf.append(optionalPrefix);
        buf.append(DEFAULT_SEPARATOR);
        buf.append(Integer.toString(h1, RADIX));
        buf.append(DEFAULT_SEPARATOR);
        buf.append(Integer.toString(h2, RADIX));
        if (time >= 0) {
            buf.append(TIME_SEPARATOR);
            buf.append(Integer.toString(time, RADIX));
        }
        return buf.toString();
    }

    /**
     * @return The first hash
     * @throws NumberFormatException
     *             if the first part (perhaps a bogus optionalPrefix) can not be parsed into an integer....
     */
    public int getH0() {
        return Integer.parseInt(optionalPrefix, RADIX);
    }

    /**
     * @return The second hash
     */
    public int getH1() {
        return h1;
    }

    /**
     * @return The third hash
     */
    public int getH2() {
        return h2;
    }

    /**
     * Get the portion of the UID to be used for sharding (@see datawave.ingest.mapreduce.handler.shard.ShardIdFactory)
     *
     * @return UID for sharding
     */
    public String getShardedPortion() {
        return getBaseUid();
    }

    /**
     * Get the optional time component
     *
     * @return The time component, -1 if not set
     */
    public int getTime() {
        return time;
    }

    /**
     * Hash the data
     *
     * @param data
     *            - the byte array to hash
     */
    private void hash(byte[] data) {
        if (optionalPrefix == null) {
            optionalPrefix = Integer.toString(hash.hash(data, data.length, SEED0), RADIX);
        }
        h1 = hash.hash(data, data.length, SEED1);
        h2 = hash.hash(data, data.length, SEED2);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(19, 37).append(this.optionalPrefix).append(this.h1).append(this.h2).append(this.time).append(this.extra).toHashCode();
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static UIDBuilder<UID> newBuilder(final Date time) {
        return (UIDBuilder) new HashUIDBuilder(time);
    }

    /**
     * Parses the string representation of the hash
     *
     * @param s
     *            - the string to parse
     * @return UID
     */
    @SuppressWarnings("unchecked")
    public static HashUID parse(String s) {
        return parse(s, -1);
    }

    /**
     * Parses the string representation of the hash, but only include up to maxExtraParts of the portion past the base hash
     *
     * @param s
     *            the string version of the hash
     * @param maxExtraParts
     *            is the number of pieces of the extra portion to include. -1 means all, 0 means none.
     * @return UID
     */
    @SuppressWarnings("unchecked")
    public static HashUID parse(String s, int maxExtraParts) {
        String[] parts = StringUtils.split(s, DEFAULT_SEPARATOR);
        if (parts.length < 3)
            throw new IllegalArgumentException("Not a valid object.");
        String p = (NULL.equals(parts[0])) ? null : parts[0];
        int a = Integer.parseInt(parts[1], RADIX);
        int timeIndex = parts[2].indexOf(TIME_SEPARATOR);
        int time = -1;
        int b = 0;
        if (timeIndex >= 0) {
            time = Integer.parseInt(parts[2].substring(timeIndex + 1), RADIX);
            b = Integer.parseInt(parts[2].substring(0, timeIndex), RADIX);
        } else {
            b = Integer.parseInt(parts[2], RADIX);
        }
        if (parts.length > 3 && maxExtraParts != 0) {
            StringBuilder extra = new StringBuilder();
            extra.append(parts[3]);
            // if maxExtraParts is negative, then including all extra parts
            int limit = parts.length;
            if (maxExtraParts > 0) {
                limit = Math.min(maxExtraParts + 3, parts.length);
            }
            for (int i = 4; i < limit; i++) {
                extra.append(DEFAULT_SEPARATOR).append(parts[i]);
            }
            return new HashUID(p, a, b, time, extra.toString());
        } else {
            return new HashUID(p, a, b, time);
        }
    }

    /**
     * Parses the string representation of the hash, but only the base part
     *
     * @param s
     *            string representation of hash
     * @return UID
     */
    @SuppressWarnings("unchecked")
    public static HashUID parseBase(String s) {
        return parse(s, 0);
    }

    public void readFields(DataInput in) throws IOException {
        HashUID input = HashUID.parse(in.readUTF());
        this.optionalPrefix = input.optionalPrefix;
        this.h1 = input.h1;
        this.h2 = input.h2;
        this.time = input.time;
        this.extra = input.extra;
    }

    @Override
    public String toString() {
        if (toString == null) {
            StringBuilder buf = new StringBuilder();
            buf.append(optionalPrefix);
            buf.append(DEFAULT_SEPARATOR);
            buf.append(Integer.toString(h1, RADIX));
            buf.append(DEFAULT_SEPARATOR);
            buf.append(Integer.toString(h2, RADIX));
            if (time >= 0) {
                buf.append(TIME_SEPARATOR);
                buf.append(Integer.toString(time, RADIX));
            }
            if (extra != null) {
                buf.append(DEFAULT_SEPARATOR);
                buf.append(extra);
            }
            toString = buf.toString();
        }
        return toString;
    }
}
