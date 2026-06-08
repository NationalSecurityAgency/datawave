package datawave.ingest.table.aggregator.util;

import java.util.BitSet;

import org.apache.accumulo.core.data.Key;

/**
 * Holds common methods for the implementations of {@link IndexKeyParser}
 */
public abstract class AbstractIndexKeyParser implements IndexKeyParser {

    protected final char NULL_CHAR = '\u0000';
    protected final char UNDERSCORE_CHAR = '_';

    protected Key key;
    protected String row;
    protected String cf;
    protected String cq;

    // indices are used to determine what type of index key the parser is operating on
    private int rowNullIndex = -1;
    protected int cqNullIndex = -1;
    protected int cqUnderscoreIndex = -1;

    protected BitSet bitset;

    public void parse(Key key) {
        clearState();
        this.key = key;
        parseRow();
        parseColumnQualifier();
    }

    private void clearState() {
        this.key = null;
        this.row = null;
        this.cq = null;
        this.cf = null;
        this.rowNullIndex = -1;
        this.cqNullIndex = -1;
        this.cqUnderscoreIndex = -1;
        this.bitset = null;
    }

    /**
     * Standard shard index keys take the form
     *
     * <pre>
     *     value FIELD:yyyyMMdd_shard0x00datatype (uid list)
     * </pre>
     *
     * @return true if the key is a standard shard index key
     */
    @Override
    public boolean isStandardKey() {
        return cqUnderscoreIndex != -1 && cqNullIndex != -1;
    }

    /**
     * Truncated shard index keys take the form
     *
     * <pre>
     *     value FIELD:yyyyMMdd0x00datatype (bitset of shard offsets)
     * </pre>
     *
     * @return true if the key is a truncated shard index key
     */
    @Override
    public boolean isTruncatedKey() {
        return cqUnderscoreIndex == -1 && cqNullIndex == 8;
    }

    /**
     * Sharded day index keys take the form
     *
     * <pre>
     *     yyyyMMdd0x00value FIELD:datatype (bitset of shard offsets)
     * </pre>
     *
     * @return true if the key is a sharded index key
     */
    @Override
    public boolean isShardedDayKey() {
        return rowNullIndex == 8 && cqUnderscoreIndex == -1 && cqNullIndex == -1;
    }

    /**
     * Sharded year index keys take the form
     *
     * <pre>
     *     yyyy0x00value FIELD:datatype (bitset of day offsets)
     * </pre>
     *
     * @return true if the key is a sharded year index key
     */
    @Override
    public boolean isShardedYearKey() {
        return rowNullIndex == 4 && cqUnderscoreIndex == -1 && cqNullIndex == -1;
    }

    public String getValue() {
        if (isStandardKey() || isTruncatedKey()) {
            return row;
        } else if (isShardedDayKey() || isShardedYearKey()) {
            return row.substring(rowNullIndex + 1);
        }
        throw new IllegalStateException("unknown shard index key structure: " + key.toString());
    }

    public String getField() {
        if (cf == null) {
            cf = key.getColumnFamily().toString();
        }
        return cf;
    }

    public String getDate() {
        if (isStandardKey()) {
            return cq.substring(0, cqUnderscoreIndex);
        } else if (isTruncatedKey()) {
            return cq.substring(0, cqNullIndex);
        } else if (isShardedDayKey() || isShardedYearKey()) {
            return row.substring(0, rowNullIndex);
        }
        throw new IllegalStateException("unknown shard index key structure: " + key.toString());
    }

    public String getDateAndShard() {
        if (isStandardKey() || isTruncatedKey()) {
            return cq.substring(0, cqNullIndex);
        } else if (isShardedDayKey() || isShardedYearKey()) {
            return row.substring(0, rowNullIndex);
        }
        throw new IllegalStateException("unknown shard index key structure: " + key.toString());
    }

    public String getDatatype() {
        return cq.substring(cqNullIndex + 1);
    }

    /**
     * Parses the row and records the presence of the first null byte. This information is used in conjunction with the {@link #cqNullIndex} to determine the
     * index key version.
     */
    protected void parseRow() {
        row = key.getRow().toString();
        char[] charArray = row.toCharArray();
        for (int i = 0; i < charArray.length; i++) {
            if (charArray[i] == NULL_CHAR) {
                rowNullIndex = i;
                break;
            }
        }
    }

    /**
     * The column qualifier may or may not contain a null byte or underscore depending on the type of index key.
     * <p>
     * A standard index key's column qualifier will always contain a null byte and an underscore
     * <p>
     * A truncated index key's column qualifier will always contain a null byte and never contain an underscore
     * <p>
     * A sharded day or year index key's column qualifier will never contain a null byte or an underscore
     */
    protected void parseColumnQualifier() {
        cq = key.getColumnQualifier().toString();
        char[] charArray = cq.toCharArray();
        for (int i = 0; i < charArray.length; i++) {
            switch (charArray[i]) {
                case UNDERSCORE_CHAR:
                    cqUnderscoreIndex = i;
                    break;
                case NULL_CHAR:
                    cqNullIndex = i;
                    break;
                default:
                    // keep going
            }
        }
    }
}
