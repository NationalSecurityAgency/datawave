package datawave.edge.util;

import java.nio.charset.CharacterCodingException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * This is an abstraction of the keys found in the protobuf edge table.
 *
 */
public class EdgeKey {
    private final EDGE_FORMAT format;
    private final STATS_TYPE statsType;
    // rowid parts
    private final String sourceData;
    private final String sinkData;
    // colf parts
    private final String type;
    private final String sourceRelationship;
    private final String sinkRelationship;
    // colq
    private final String sourceAttribute1;
    private final String sinkAttribute1;
    private final String attribute2;
    private final String attribute3;
    private final String yyyymmdd;
    private final DATE_TYPE dateType;

    // colv
    private final Text colvis;
    // other key parts
    private final long timestamp;
    private final boolean deleted;

    private static final Logger log = Logger.getLogger(EdgeKey.class);

    // use the builder, not this nightmare constructor
    private EdgeKey(EDGE_FORMAT format, STATS_TYPE statsType, String sourceData, String sinkData, String family, String sourceRelationship,
                    String sinkRelationship, String sourceAttribute1, String sinkAttribute1, String yyyymmdd, String attribute3, String attribute2, Text colvis,
                    long timestamp, boolean deleted, DATE_TYPE dateType) {
        this.format = format;
        this.statsType = statsType;
        this.sourceData = sourceData;
        this.sinkData = sinkData;
        this.type = family;
        this.sourceRelationship = sourceRelationship;
        this.sinkRelationship = sinkRelationship;
        this.sourceAttribute1 = sourceAttribute1;
        this.sinkAttribute1 = sinkAttribute1;
        this.yyyymmdd = yyyymmdd;
        this.attribute2 = attribute2;
        this.attribute3 = attribute3;
        this.dateType = dateType;
        this.colvis = colvis;
        this.timestamp = timestamp;
        this.deleted = deleted;
    }

    public static class EdgeKeyBuilder {
        private EDGE_FORMAT format;
        private STATS_TYPE statsType;
        // rowid parts
        private String sourceData;
        private String sinkData;
        // colf parts
        private String type;
        private String sourceRelationship;
        private String sinkRelationship;
        // colq
        private String sourceAttribute1;
        private String sinkAttribute1;
        private String attribute2;
        private String attribute3;
        private String yyyymmdd;
        private DATE_TYPE dateType;
        // colv
        private Text colvis;

        // Other key parts
        private long timestamp = Long.MAX_VALUE;
        private boolean deleted = false;

        private static final String EMPTY = "";
        private boolean escape;
        private boolean unescape;

        private EdgeKeyBuilder() {
            colvis = new Text();
            clearFields();
        }

        private EdgeKeyBuilder(EdgeKey key) {
            escape = false;
            format = key.getFormat();
            statsType = key.getStatsType();
            sourceData = key.getSourceData();
            sinkData = key.getSinkData();
            type = key.getType();
            unescape = false;
            sourceRelationship = key.getSourceRelationship();
            sinkRelationship = key.getSinkRelationship();
            sourceAttribute1 = key.getSourceAttribute1();
            sinkAttribute1 = key.getSinkAttribute1();
            attribute2 = key.getAttribute2();
            attribute3 = key.getAttribute3();
            yyyymmdd = key.getYyyymmdd();
            dateType = key.getDateType();
            colvis = new Text(key.getColvis());
            timestamp = key.getTimestamp();
            deleted = key.isDeleted();
        }

        public EdgeKeyBuilder clearFields() {
            format = EDGE_FORMAT.STANDARD;
            escape = false;
            unescape = false;
            deleted = false;
            statsType = STATS_TYPE.ACTIVITY;
            sourceData = "";
            sinkData = "";
            type = "";
            sourceRelationship = "";
            sinkRelationship = "";
            sourceAttribute1 = "";
            sinkAttribute1 = "";
            attribute2 = "";
            attribute3 = "";
            yyyymmdd = "";
            dateType = DATE_TYPE.OLD_EVENT;
            colvis.clear();
            timestamp = Long.MAX_VALUE;
            return this;
        }

        /**
         * Builds the edge Key using the provided information. If the escape/unescape sequence fails, we will rely on the original data, since the decode/decode
         * sequence will succeed nonetheless.
         *
         * @return the built edge key.
         */
        public EdgeKey build() {

            String tempSourceData = this.sourceData;
            String tempSinkData = this.sinkData;
            try {
                if (log.isTraceEnabled()) {
                    log.trace("Attempting escape sequencing isEscape? " + escape + " isUnescape? " + unescape);
                    log.trace("Values before attempt source data " + tempSourceData + ", sink data " + tempSinkData);
                }
                if (escape && !unescape) {
                    tempSourceData = StringEscapeUtils.escapeJava(sourceData);
                    tempSinkData = StringEscapeUtils.escapeJava(sinkData);
                } else if (unescape && !escape) {

                    tempSourceData = StringEscapeUtils.unescapeJava(sourceData);
                    tempSinkData = StringEscapeUtils.unescapeJava(sinkData);

                }

                // moving the assignment here since we want to rely on the original data
                // if for some reason either of the escape/unescape sequence fails
                this.sourceData = tempSourceData;
                this.sinkData = tempSinkData;

            } catch (Exception e) {

                log.error("Avoiding escape sequencing, due to : " + e);

            }

            return new EdgeKey(getFormat(), getStatsType(), getSourceData(), getSinkData(), getType(), getSourceRelationship(), getSinkRelationship(),
                            getSourceAttribute1(), getSinkAttribute1(), getYyyymmdd(), getAttribute3(), getAttribute2(), getColvis(), getTimestamp(),
                            isDeleted(), getDateType());
        }

        public EDGE_FORMAT getFormat() {
            return format;
        }

        public EdgeKeyBuilder setFormat(EDGE_FORMAT format) {
            this.format = format;
            return this;
        }

        public STATS_TYPE getStatsType() {
            return statsType;
        }

        public EdgeKeyBuilder setStatsType(STATS_TYPE type) {
            this.statsType = type;
            return this;
        }

        public String getSourceData() {
            return (null == sourceData) ? EMPTY : sourceData;
        }

        public EdgeKeyBuilder setSourceData(String sourceData) {

            this.sourceData = sourceData;

            return this;
        }

        public String getSinkData() {
            return (null == sinkData) ? EMPTY : sinkData;
        }

        public EdgeKeyBuilder setSinkData(String sinkData) {

            this.sinkData = sinkData;

            return this;
        }

        public String getType() {
            return (null == type) ? EMPTY : type;
        }

        public EdgeKeyBuilder setType(String type) {
            this.type = type;
            return this;
        }

        public EdgeKeyBuilder escape() {
            this.escape = true;
            this.unescape = false;
            return this;
        }

        public EdgeKeyBuilder unescape() {
            this.unescape = true;
            this.escape = false;
            return this;
        }

        public String getSourceRelationship() {
            return (null == sourceRelationship) ? EMPTY : sourceRelationship;
        }

        public EdgeKeyBuilder setSourceRelationship(String sourceRelationship) {
            this.sourceRelationship = sourceRelationship;
            return this;
        }

        public String getSinkRelationship() {
            return (null == sinkRelationship) ? EMPTY : sinkRelationship;
        }

        public EdgeKeyBuilder setSinkRelationship(String sinkRelationship) {
            this.sinkRelationship = sinkRelationship;
            return this;
        }

        public String getSourceAttribute1() {
            return (null == sourceAttribute1) ? EMPTY : sourceAttribute1;
        }

        public EdgeKeyBuilder setSourceAttribute1(String sourceAttribute1) {
            this.sourceAttribute1 = sourceAttribute1;
            return this;
        }

        public String getSinkAttribute1() {
            return (null == sinkAttribute1) ? EMPTY : sinkAttribute1;
        }

        public EdgeKeyBuilder setSinkAttribute1(String sinkAttribute1) {
            this.sinkAttribute1 = sinkAttribute1;
            return this;
        }

        public String getAttribute2() {
            return (null == attribute2) ? EMPTY : attribute2;
        }

        public EdgeKeyBuilder setAttribute2(String attribute2) {
            this.attribute2 = attribute2;
            return this;
        }

        public String getAttribute3() {
            return (null == attribute3) ? EMPTY : attribute3;
        }

        public EdgeKeyBuilder setAttribute3(String attribute3) {
            this.attribute3 = attribute3;
            return this;
        }

        public String getYyyymmdd() {
            return (null == yyyymmdd) ? EMPTY : yyyymmdd;
        }

        public DATE_TYPE getDateType() {
            return dateType;
        }

        public void setDateType(DATE_TYPE dateType) {
            this.dateType = dateType;
        }

        public EdgeKeyBuilder setYyyymmdd(String yyyymmdd) {
            this.yyyymmdd = yyyymmdd;
            return this;
        }

        public Text getColvis() {
            return (null == colvis) ? new Text() : colvis;
        }

        public EdgeKeyBuilder setColvis(Text colvis) {
            this.colvis = new Text(colvis);
            return this;
        }

        public EdgeKeyBuilder setColvis(ColumnVisibility colvis) {
            this.colvis = new Text(colvis.getExpression());
            return this;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public EdgeKeyBuilder setTimestamp(long timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public boolean isDeleted() {
            return deleted;
        }

        public EdgeKeyBuilder setDeleted(boolean deleted) {
            this.deleted = deleted;
            return this;
        }

        @Override
        public int hashCode() {
            final int prime = 223;
            int result = 1;
            result = prime * result + ((colvis == null) ? 0 : colvis.hashCode());
            result = prime * result + (deleted ? 1231 : 1237);
            result = prime * result + ((format == null) ? 0 : format.hashCode());
            result = prime * result + ((type == null) ? 0 : type.hashCode());
            result = prime * result + ((sinkData == null) ? 0 : sinkData.hashCode());
            result = prime * result + ((sinkRelationship == null) ? 0 : sinkRelationship.hashCode());
            result = prime * result + ((sinkAttribute1 == null) ? 0 : sinkAttribute1.hashCode());
            result = prime * result + ((sourceData == null) ? 0 : sourceData.hashCode());
            result = prime * result + ((sourceRelationship == null) ? 0 : sourceRelationship.hashCode());
            result = prime * result + ((sourceAttribute1 == null) ? 0 : sourceAttribute1.hashCode());
            result = prime * result + ((statsType == null) ? 0 : statsType.hashCode());
            result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
            result = prime * result + ((attribute3 == null) ? 0 : attribute3.hashCode());
            result = prime * result + ((attribute2 == null) ? 0 : attribute2.hashCode());
            result = prime * result + ((yyyymmdd == null) ? 0 : yyyymmdd.hashCode());
            result = prime * result + ((dateType == null) ? 0 : dateType.hashCode());
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            EdgeKeyBuilder other = (EdgeKeyBuilder) obj;
            if (colvis == null) {
                if (other.colvis != null)
                    return false;
            } else if (!colvis.equals(other.colvis))
                return false;
            if (deleted != other.deleted)
                return false;
            if (format != other.format)
                return false;
            if (type == null) {
                if (other.type != null)
                    return false;
            } else if (!type.equals(other.type))
                return false;
            if (sinkData == null) {
                if (other.sinkData != null)
                    return false;
            } else if (!sinkData.equals(other.sinkData))
                return false;
            if (sinkRelationship == null) {
                if (other.sinkRelationship != null)
                    return false;
            } else if (!sinkRelationship.equals(other.sinkRelationship))
                return false;
            if (sinkAttribute1 == null) {
                if (other.sinkAttribute1 != null)
                    return false;
            } else if (!sinkAttribute1.equals(other.sinkAttribute1))
                return false;
            if (sourceData == null) {
                if (other.sourceData != null)
                    return false;
            } else if (!sourceData.equals(other.sourceData))
                return false;
            if (sourceRelationship == null) {
                if (other.sourceRelationship != null)
                    return false;
            } else if (!sourceRelationship.equals(other.sourceRelationship))
                return false;
            if (sourceAttribute1 == null) {
                if (other.sourceAttribute1 != null)
                    return false;
            } else if (!sourceAttribute1.equals(other.sourceAttribute1))
                return false;
            if (statsType != other.statsType)
                return false;
            if (timestamp != other.timestamp)
                return false;
            if (attribute2 == null) {
                if (other.attribute2 != null)
                    return false;
            } else if (!attribute2.equals(other.attribute2))
                return false;
            if (attribute3 == null) {
                if (other.attribute3 != null)
                    return false;
            } else if (!attribute3.equals(other.attribute3))
                return false;
            if (yyyymmdd == null) {
                if (other.yyyymmdd != null)
                    return false;
            } else if (!yyyymmdd.equals(other.yyyymmdd))
                return false;
            if (dateType == null) {
                if (other.dateType != null) {
                    return false;
                }
            } else if (!dateType.equals(other.dateType)) {
                return false;
            }
            return true;
        }
    } // End of EdgeKeyBuilder

    public static EdgeKeyBuilder newBuilder() {
        return new EdgeKeyBuilder();
    }

    public static EdgeKeyBuilder newBuilder(EdgeKey edgeKey) {
        return new EdgeKeyBuilder(edgeKey);
    }

    public static EdgeKeyBuilder newBuilder(EdgeKey.EDGE_FORMAT format) {
        EdgeKeyBuilder builder = EdgeKey.newBuilder();
        return builder.setFormat(format);
    }

    // Generate a new key with the source and sink stuff swapped
    public static EdgeKey swapSourceSink(EdgeKey swap) {
        EdgeKeyBuilder builder = newBuilder(swap);
        builder.setSourceData(swap.getSinkData());
        builder.setSinkData(swap.getSourceData());
        builder.setSourceRelationship(swap.getSinkRelationship());
        builder.setSinkRelationship(swap.getSourceRelationship());
        builder.setSourceAttribute1(swap.getSinkAttribute1());
        builder.setSinkAttribute1(swap.getSourceAttribute1());

        return builder.build();
    }

    private static final String EDGE_METADATA_COLUMN = "edge";

    private static final String STATS_COLF = "STATS";

    static final byte[] STATS_BYTES = STATS_COLF.getBytes();

    private static final int SOURCE_INDEX = 0;
    private static final int SINK_INDEX = 1;

    public static final char COL_SEPARATOR = '/';
    public static final String COL_SEPARATOR_STR = String.valueOf(COL_SEPARATOR);
    public static final byte COL_SEPARATOR_BYTE = '/';
    public static final char COL_SUB_SEPARATOR = '-';

    public enum STATS_TYPE {
        DURATION, ACTIVITY, LINKS;

        public static STATS_TYPE getStatsType(String statsLabel) {
            if (DURATION.name().equals(statsLabel)) {
                return DURATION;
            } else if (ACTIVITY.name().equals(statsLabel)) {
                return ACTIVITY;
            } else if (LINKS.name().equals(statsLabel)) {
                return LINKS;
            } else {
                throw new EnumConstantNotPresentException(STATS_TYPE.class, statsLabel);
            }
        }

        public static int getMaxLength() {
            return 8;
        }
    }

    /**
     * Note that keys within DataWave's edge model typically have a date value encoded within them. As such, this enum allows that date value to be further
     * qualified, wrt the source "event" from which the edge key was derived.
     *
     * <p>
     * Thus, the {@link DATE_TYPE#abbreviation} field here is written into the edge key as a qualifier for the date value to enable greater flexibility in
     * date-filtering logic, should clients need it.
     *
     * <p>
     * An "EVENT*" type here denotes that the date within the edge key originated from the raw source event's date field, ie, from
     * datawave.ingest.data.RawRecordContainer.getDate, at ingest time. In the DW data model, this date represents the date portion of the Accumulo row id for
     * the source record within DataWave's shard table.
     *
     * <p>
     * Likewise, an "ACTIVITY*" type is simply a general term denoting that date in the edge key is associated with some other date value within the raw record,
     * not the event date (or 'shard date').
     */
    public enum DATE_TYPE {
        ACTIVITY_ONLY("C"), EVENT_ONLY("A"), ACTIVITY_AND_EVENT("B"), OLD_EVENT("");

        String abbreviation;

        DATE_TYPE(String character) {
            this.abbreviation = character;
        }

        public static DATE_TYPE parse(String dateType) {
            for (DATE_TYPE dType : DATE_TYPE.values()) {
                if (dType.abbreviation.equals(dateType)) {
                    return dType;
                }
            }
            return null; // default is unidirectional
        }

        @Override
        public String toString() {
            return abbreviation;
        }

    }

    public enum EDGE_FORMAT {
        STANDARD(2), STATS(1), UNKNOWN(0);

        private final int splitLength;

        private EDGE_FORMAT(int splitLength) {
            this.splitLength = splitLength;
        }

        public static EDGE_FORMAT getFormatFromRow(int splitLength) {
            if (splitLength == STANDARD.splitLength) {
                return STANDARD;
            } else if (splitLength == STATS.splitLength) {
                return STATS;
            } else {
                return UNKNOWN;
            }
        }
    }

    /*
     * for determining what version of edge is being parsed as the table evolves need to be careful that we don't make it so we can't tell the difference
     * between an older edge and a newer edge
     *
     * also, it keeps the index numbering nightmare in a single place.
     *
     * indices assume colf and colq are in a single list.
     */
    public enum EDGE_VERSION {
        /* 0 1 2 3 4 5 6 7 */

        STATS_BASE(5, 1, 2, 3, 4, 5, -1, -1, -1, EDGE_FORMAT.STATS, false, false), /* STATS / STATTYPE / TYPE / RELATIONSHIP / CATEGORY : YYYYMMDD */
        STATS_ATTRIBUTE2(6, 1, 2, 3, 4, 6, 5, -1, -1, EDGE_FORMAT.STATS, true, false), /*
                                                                                        * STATS / STATTYPE / TYPE / RELATIONSHIP / CATEGORY / ATTRIBUTE2 :
                                                                                        * YYYYMMDD
                                                                                        */
        BASE(3, 1, 0, 1, 2, 3, -1, -1, -1, EDGE_FORMAT.STANDARD, false, false), /* TYPE / RELATIONSHIP / CATEGORY : YYYYMMDD */
        BASE_ATTRIBUTE2(4, 1, 0, 1, 2, 4, 3, -1, -1, EDGE_FORMAT.STANDARD, true, false), /* TYPE / RELATIONSHIP / CATEGORY / ATTRIBUTE2 : YYYYMMDD */
        STATS_PROTOBUF(4, 4, 2, 3, 5, 4, 6, 7, -1, EDGE_FORMAT.STATS, true, true), /*
                                                                                    * STATS / STATTYPE / TYPE / RELATIONSHIP : YYYYMMDD / CATEGORY / ATTRIBUTE2
                                                                                    * / ATTRIBUTE3
                                                                                    */
        DATE_STATS_PROTOBUF(4, 5, 2, 3, 5, 4, 6, 7, 8, EDGE_FORMAT.STATS, true, true), /*
                                                                                        * STATS / STATTYPE / TYPE / RELATIONSHIP : YYYYMMDD / CATEGORY /
                                                                                        * ATTRIBUTE2 / ATTRIBUTE3 / DATETYPE
                                                                                        */
        PROTOBUF(2, 4, 0, 1, 3, 2, 4, 5, -1, EDGE_FORMAT.STANDARD, true, true), /* TYPE / RELATIONSHIP : YYYYMMDD / CATEGORY / ATTRIBUTE2 / ATTRIBUTE3 */
        DATE_PROTOBUF(2, 5, 0, 1, 3, 2, 4, 5, 6, EDGE_FORMAT.STANDARD, true, true), /*
                                                                                     * TYPE / RELATIONSHIP : YYYYMMDD / CATEGORY / ATTRIBUTE2 / ATTRIBUTE3
                                                                                     * /DATETYPE
                                                                                     */
        UNKNOWN(-1, -1, -1, -1, -1, -1, -1, -1, -1, EDGE_FORMAT.STANDARD, false, false);

        private final int ncf;
        private final int ncq;
        private final int iType;
        private final int iRelationship;
        private final int iCategory;
        private final int iYYYYMMDD;
        private final int iAttribute2;
        private final int iAttribute3;
        private final int iDateType;
        private final EDGE_FORMAT format;
        private final boolean hasAttribute2;
        private final boolean hasAttribute3;
        private final int iStatsType = 1;

        private EDGE_VERSION(int ncf, int ncq, int iType, int iRelationship, int iCategory, int iYYYYMMDD, int iAttribute2, int iAttribute3, int iDateType,
                        EDGE_FORMAT format, boolean hasAttribute2, boolean hasAttribute3) {

            this.ncf = ncf;
            this.ncq = ncq;
            this.iType = iType;
            this.iRelationship = iRelationship;
            this.iCategory = iCategory;
            this.iYYYYMMDD = iYYYYMMDD;
            this.iAttribute2 = iAttribute2;
            this.iAttribute3 = iAttribute3;
            this.iDateType = iDateType;
            this.format = format;
            this.hasAttribute2 = hasAttribute2;
            this.hasAttribute3 = hasAttribute3;
        }

        public int getNumColfPieces() {
            return ncf;
        }

        public int getNumColqPieces() {
            return ncq;
        }

        public int getTotalColPieces() {
            return ncf + ncq;
        }

        public int getTypeIndex() {
            return iType;
        }

        public int getStatsTypeIndex() {
            return iStatsType;
        }

        public int getRelationshipIndex() {
            return iRelationship;
        }

        public int getCategoryIndex() {
            return iCategory;
        }

        public int getYMDIndex() {
            return iYYYYMMDD;
        }

        public int getAttribute2Index() {
            return iAttribute2;
        }

        public int getAttribute3Index() {
            return iAttribute3;
        }

        public int getDateTypeIndex() {
            return iDateType;
        }

        public EDGE_FORMAT getFormat() {
            return format;
        }

        public boolean hasAttribute2() {
            return hasAttribute2;
        }

        public boolean hasAttribute3() {
            return hasAttribute3;
        }

        public STATS_TYPE getStatsType(List<String> pieces) {
            return STATS_TYPE.getStatsType(pieces.get(iStatsType));
        }

        public static EDGE_VERSION getEdgeVersion(List<String> pieces) {
            int nPieces = pieces.size();
            if (nPieces < 4) {
                return UNKNOWN;
            }
            if (pieces.get(0).equals(STATS_COLF)) {
                if (nPieces == 6) {
                    return STATS_BASE;
                } else if (nPieces == 7) {
                    return STATS_ATTRIBUTE2;
                } else if (nPieces == 8) {
                    return STATS_PROTOBUF;
                } else if (nPieces == 9) {
                    return DATE_STATS_PROTOBUF;
                }
            } else {
                if (nPieces == 4) {
                    return BASE;
                } else if (nPieces == 5) {
                    return BASE_ATTRIBUTE2;
                } else if (nPieces == 6) {
                    return PROTOBUF;
                } else if (nPieces == 7) {
                    return DATE_PROTOBUF;
                }
            }
            return UNKNOWN;
        }
    }

    /**
     * An abstraction of the column family and qualifier pieces and parts.
     */
    public static class EdgeColumnParts extends AbstractList<String> implements List<String> {
        private String[] parts = new String[9];
        private int pLen = 0;

        /**
         * This constructor is preferred because it allows the client to reuse Text objects to avoid constructing and destructing Text objects
         *
         * @param colFam
         *            the column family text
         * @param colQual
         *            the column qualifier text
         */
        public EdgeColumnParts(Text colFam, Text colQual) {
            getParts(colFam);
            getParts(colQual);
        }

        /**
         * This constructor should be avoided because it creates two new Text objects each time it is called
         *
         * @param key
         *            the edge key
         */

        public EdgeColumnParts(Key key) {
            getParts(key.getColumnFamilyData());
            getParts(key.getColumnQualifierData());
        }

        private void getParts(Text text) {
            getParts(text.getBytes(), text.getLength());
        }

        /**
         * @param bytes
         *            byte array holding the parts of the edge key
         * @param bLen
         *            number of bytes to use (important: the byte array may be reused so its length may not be correct)
         */
        private void getParts(byte[] bytes, int bLen) {
            try {
                int start = 0;
                for (int i = 0; i < bLen; i++) {
                    if (pLen >= parts.length) {
                        throw new RuntimeException("Exceeded number of possible number of parts (" + parts.length + ")." + "  bytes as String: "
                                        + new Text(bytes) + " parts: " + Arrays.toString(parts));
                    }
                    if (bytes[i] == COL_SEPARATOR_BYTE) {
                        parts[pLen++] = Text.decode(bytes, start, i - start);
                        start = i + 1;
                    }
                }
                parts[pLen++] = Text.decode(bytes, start, bLen - start);

            } catch (CharacterCodingException e) {
                throw new RuntimeException("Edge key column encoding exception", e);
            }
        }

        private void getParts(ByteSequence byteSeq) {
            byte[] bytes = byteSeq.getBackingArray();
            getParts(bytes, byteSeq.length());
        }

        @Override
        public int size() {
            return pLen;
        }

        @Override
        public String get(int pos) {
            return parts[pos];
        }
    }

    /**
     * Parses edge table keys into the various fields encoded within the key. The source and sink are unescaped, ready to send to external clients.
     *
     * @param key
     *            a key from the Datawave edge table
     * @return an immutable EdgeKey object
     */
    public static EdgeKey decode(Key key) {
        return decode(key, EdgeKey.newBuilder().unescape());
    }

    /**
     * Decode the key leaving the row portion native to the accumulo key, leaving the source and sink as they are in the accumulo row. Iterators which create
     * new keys without the builders, can safely decode with this method and reseek to the same position, without knowledge of the underlying escaping encodings
     *
     * @param key
     *            a key from the Datawave edge table
     * @return an immutable EdgeKey object
     */
    public static EdgeKey decodeForInternal(Key key) {
        EdgeKeyBuilder builder = EdgeKey.newBuilder();
        builder.unescape = false;
        builder.escape = false;
        return decode(key, builder);
    }

    protected static EdgeKey decode(Key key, EdgeKeyBuilder builder) {
        EdgeKeyDecoder edgeKeyDecoder = new EdgeKeyDecoder(); // to maintain method's static modifier
        return edgeKeyDecoder.decode(key, builder);
    }

    private Key encode(EDGE_VERSION version) {
        Text rowid = null;
        Text colf = null;
        Text colq = null;
        Text colvis = null;
        boolean deleted = false;
        long timestamp = Long.MAX_VALUE;

        List<String> parts = new ArrayList<>(version.getTotalColPieces());
        for (int ii = 0; ii < version.getTotalColPieces(); ii++) {
            parts.add("null");
        }

        // row id
        StringBuilder rowsb = new StringBuilder();
        if (this.format == EDGE_FORMAT.STATS) {
            rowsb.append(this.getSourceData());
            parts.set(0, STATS_COLF);
            parts.set(version.getStatsTypeIndex(), this.getStatsType().name());
        } else if (this.format == EDGE_FORMAT.STANDARD) {
            rowsb.append(this.getSourceData()).append("\0").append(this.getSinkData());
        } else {
            throw new IllegalStateException("Invalid Edge Type encountered: " + this.format);
        }
        rowid = new Text(rowsb.toString());

        // populate the parts array according to the version
        parts.set(version.getTypeIndex(), this.getType());
        parts.set(version.getRelationshipIndex(), this.getRelationship());
        parts.set(version.getCategoryIndex(), this.getAttribute1());
        parts.set(version.getYMDIndex(), this.getYyyymmdd());
        if (version.hasAttribute2()) {
            parts.set(version.getAttribute2Index(), this.getAttribute2());
        }
        ;
        if (version.hasAttribute3()) {
            parts.set(version.getAttribute3Index(), this.getAttribute3());
        }
        ;
        if (version.getDateTypeIndex() >= 0) {
            parts.set(version.getDateTypeIndex(), this.getDateType().toString());
        }

        // serialize the colf
        StringBuilder colsb = new StringBuilder(parts.get(0));
        for (int ii = 1; ii < version.getNumColfPieces(); ii++) {
            colsb.append(COL_SEPARATOR).append(parts.get(ii));
        }
        colf = new Text(colsb.toString());

        // serialize the colq
        colsb = new StringBuilder(parts.get(version.getNumColfPieces()));
        for (int ii = version.getNumColfPieces() + 1; ii < version.getTotalColPieces(); ii++) {
            colsb.append(COL_SEPARATOR).append(parts.get(ii));
        }
        colq = new Text(colsb.toString());

        // colvis
        colvis = new Text(this.getColvis());

        // timestamp
        timestamp = this.getTimestamp();

        // deleted
        deleted = this.isDeleted();

        Key key = new Key(rowid, colf, colq, colvis, timestamp);
        key.setDeleted(deleted);

        return key;
    }

    /**
     * Creates an edge table key from the various EdgeKey fields.
     *
     * @return a key for the Datawave edge table
     */
    public Key encode() {
        if (this.getDateType() == DATE_TYPE.OLD_EVENT) {
            return encodeLegacyProtobufKey();
        } else {
            if (this.getFormat() == EDGE_FORMAT.STATS) {
                return encode(EDGE_VERSION.DATE_STATS_PROTOBUF);
            } else if (this.getFormat() == EDGE_FORMAT.STANDARD) {
                return encode(EDGE_VERSION.DATE_PROTOBUF);
            } else {
                // EDGE_FORMAT.UNKNOWN
                throw new IllegalStateException("Can't encode unknown edge key format." + this);
            }
        }

    }

    public Key encodeLegacyProtobufKey() {
        if (this.getFormat() == EDGE_FORMAT.STATS) {
            return encode(EDGE_VERSION.STATS_PROTOBUF);
        } else if (this.getFormat() == EDGE_FORMAT.STANDARD) {
            return encode(EDGE_VERSION.PROTOBUF);
        } else {
            // EDGE_FORMAT.UNKNOWN
            throw new IllegalStateException("Can't encode unknown edge key format." + this);
        }
    }

    public Key encodeLegacyAttribute2Key() {
        if (this.getFormat() == EDGE_FORMAT.STATS) {
            return encode(EDGE_VERSION.STATS_ATTRIBUTE2);
        } else if (this.getFormat() == EDGE_FORMAT.STANDARD) {
            return encode(EDGE_VERSION.BASE_ATTRIBUTE2);
        } else {
            // EDGE_FORMAT.UNKNOWN
            throw new IllegalStateException("Can't encode unknown edge key format." + this);
        }
    }

    public Key encodeLegacyKey() {
        if (this.getFormat() == EDGE_FORMAT.STATS) {
            return encode(EDGE_VERSION.STATS_BASE);
        } else if (this.getFormat() == EDGE_FORMAT.STANDARD) {
            return encode(EDGE_VERSION.BASE);
        } else {
            // EDGE_FORMAT.UNKNOWN
            throw new IllegalStateException("Can't encode unknown edge key format." + this);
        }
    }

    /**
     * Creates the metadata table key entry for this EdgeKey
     *
     * @return a key object for use in the Datawave Metadata table
     */
    public Key getMetadataKey() {
        Text row = new Text(this.getType() + COL_SEPARATOR + this.getRelationship());
        Text colf = new Text(EDGE_METADATA_COLUMN);
        Text colq = new Text(this.getAttribute1());

        return new Key(row, colf, colq, new Text(""), this.getTimestamp());
    }

    /**
     * Creates the metadata table key entry for a given edge table Key
     *
     * @param key
     *            edge table key to lookup
     * @return a key object for use in the Datawave Metadata table
     */
    public static Key getMetadataKey(Key key) {
        EdgeKey eKey = EdgeKey.decode(key);
        return eKey.getMetadataKey();
    }

    // Getter nightmare below here
    public EDGE_FORMAT getFormat() {
        return format;
    }

    public STATS_TYPE getStatsType() {
        return statsType;
    }

    public String getSourceData() {
        return sourceData;
    }

    public String getSinkData() {
        return sinkData;
    }

    public String getType() {
        return type;
    }

    public String getRelationship() {
        // return a relationship string based on the edge type.
        if (this.getFormat() == EDGE_FORMAT.STANDARD) {
            return getSourceRelationship() + COL_SUB_SEPARATOR + getSinkRelationship();
        }
        return getSourceRelationship();
    }

    public String getSourceRelationship() {
        return sourceRelationship;
    }

    public String getSinkRelationship() {
        return sinkRelationship;
    }

    public String getAttribute1() {
        // return a relationship string based on the edge type.
        if ((this.getFormat() == EDGE_FORMAT.STANDARD) || ((getFormat() == EDGE_FORMAT.STATS) && this.getStatsType() == STATS_TYPE.LINKS)) {
            return getSourceAttribute1() + COL_SUB_SEPARATOR + getSinkAttribute1();
        }
        return getSourceAttribute1();
    }

    public String getSourceAttribute1() {
        return sourceAttribute1;
    }

    public String getSinkAttribute1() {
        return sinkAttribute1;
    }

    public boolean hasAttribute2() {
        return StringUtils.isNotBlank(attribute2);
    }

    public String getAttribute2() {
        return attribute2;
    }

    public boolean hasAttribute3() {
        return StringUtils.isNotBlank(attribute3);
    }

    public String getAttribute3() {
        return attribute3;
    }

    public String getYyyymmdd() {
        return yyyymmdd;
    }

    public DATE_TYPE getDateType() {
        return dateType;
    }

    public Text getColvis() {
        return colvis == null ? new Text(colvis) : colvis;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public boolean isStatsKey() {
        return (this.getFormat() == EDGE_FORMAT.STATS);
    }

    @Override
    public int hashCode() {
        final int prime = 223;
        int result = 1;
        result = prime * result + ((colvis == null) ? 0 : colvis.hashCode());
        result = prime * result + (deleted ? 1231 : 1237);
        result = prime * result + ((format == null) ? 0 : format.hashCode());
        result = prime * result + ((type == null) ? 0 : type.hashCode());
        result = prime * result + ((sinkData == null) ? 0 : sinkData.hashCode());
        result = prime * result + ((sinkRelationship == null) ? 0 : sinkRelationship.hashCode());
        result = prime * result + ((sinkAttribute1 == null) ? 0 : sinkAttribute1.hashCode());
        result = prime * result + ((sourceData == null) ? 0 : sourceData.hashCode());
        result = prime * result + ((sourceRelationship == null) ? 0 : sourceRelationship.hashCode());
        result = prime * result + ((sourceAttribute1 == null) ? 0 : sourceAttribute1.hashCode());
        result = prime * result + ((statsType == null) ? 0 : statsType.hashCode());
        result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
        result = prime * result + ((attribute3 == null) ? 0 : attribute3.hashCode());
        result = prime * result + ((attribute2 == null) ? 0 : attribute2.hashCode());
        result = prime * result + ((yyyymmdd == null) ? 0 : yyyymmdd.hashCode());
        result = prime * result + ((dateType == null) ? 0 : dateType.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        EdgeKey other = (EdgeKey) obj;
        if (colvis == null) {
            if (other.colvis != null)
                return false;
        } else if (!colvis.equals(other.colvis))
            return false;
        if (deleted != other.deleted)
            return false;
        if (format != other.format)
            return false;
        if (type == null) {
            if (other.type != null)
                return false;
        } else if (!type.equals(other.type))
            return false;
        if (sinkData == null) {
            if (other.sinkData != null)
                return false;
        } else if (!sinkData.equals(other.sinkData))
            return false;
        if (sinkRelationship == null) {
            if (other.sinkRelationship != null)
                return false;
        } else if (!sinkRelationship.equals(other.sinkRelationship))
            return false;
        if (sinkAttribute1 == null) {
            if (other.sinkAttribute1 != null)
                return false;
        } else if (!sinkAttribute1.equals(other.sinkAttribute1))
            return false;
        if (sourceData == null) {
            if (other.sourceData != null)
                return false;
        } else if (!sourceData.equals(other.sourceData))
            return false;
        if (sourceRelationship == null) {
            if (other.sourceRelationship != null)
                return false;
        } else if (!sourceRelationship.equals(other.sourceRelationship))
            return false;
        if (sourceAttribute1 == null) {
            if (other.sourceAttribute1 != null)
                return false;
        } else if (!sourceAttribute1.equals(other.sourceAttribute1))
            return false;
        if (statsType != other.statsType)
            return false;
        if (timestamp != other.timestamp)
            return false;
        if (attribute2 == null) {
            if (other.attribute2 != null)
                return false;
        } else if (!attribute2.equals(other.attribute2))
            return false;
        if (attribute3 == null) {
            if (other.attribute3 != null)
                return false;
        } else if (!attribute3.equals(other.attribute3))
            return false;
        if (yyyymmdd == null) {
            if (other.yyyymmdd != null)
                return false;
        } else if (!yyyymmdd.equals(other.yyyymmdd))
            return false;
        if (dateType == null) {
            if (other.dateType != null) {
                return false;
            }
        } else if (!dateType.equals(other.dateType)) {
            return false;
        }
        return true;
    }

    /**
     * Determine if an edge is based on event date.
     *
     * @param k
     *            edge key
     * @return True if this is edge date (YYYYMMDD) is based on event date.
     * @note An edge can be both an event and and activity edge. Hence, do not test for event edge by doing !isActivityEdge(k).
     */

    /**
     * Determine as fast as possible the date type of an edge key without having to decode into and EdgeKey
     *
     * @param key
     *            edge key
     * @return the date type of this accumulo edge key
     */
    public static DATE_TYPE getDateType(Key key) {

        EdgeColumnParts parts = new EdgeColumnParts(key);

        EDGE_VERSION version = EDGE_VERSION.getEdgeVersion(parts);

        if (version == EDGE_VERSION.DATE_STATS_PROTOBUF) {
            return DATE_TYPE.parse(parts.get(8));
        } else if (version == EDGE_VERSION.DATE_PROTOBUF) {
            return DATE_TYPE.parse(parts.get(6));
        } else {
            return DATE_TYPE.OLD_EVENT;
        }
    }
}
