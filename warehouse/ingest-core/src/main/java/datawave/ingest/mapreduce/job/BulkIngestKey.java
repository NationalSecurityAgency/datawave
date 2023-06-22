package datawave.ingest.mapreduce.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import org.apache.accumulo.core.data.Key;

/**
 * Used during bulk ingest to convey the table name to the reducer and stores the key for sorting.
 * <p>
 * <strong>Note:</strong> For serialization and binary comparison, this class does not handle keys with the deleted flag set. This should not be a problem for
 * ingest of new data.
 */
public class BulkIngestKey implements WritableComparable<BulkIngestKey> {

    protected Text tableName = null;
    protected Key key = new Key();
    // computed hashcode. we won't write this through the writable interface
    // to avoid increasing the size of our spilled data
    protected int hashCode = 31;

    public BulkIngestKey() {
        this.tableName = new Text();
        buildHashCode();
    }

    public BulkIngestKey(Text tableName, Key key) {
        super();
        this.tableName = tableName;
        if (null == this.tableName) {
            this.tableName = new Text();
        }
        this.key = key;
        buildHashCode();
    }

    public Text getTableName() {
        return tableName;
    }

    public Key getKey() {
        return key;
    }

    /**
     * Build the computed hash code.
     */
    protected void buildHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        hashCode = result;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        tableName = new Text(readText(in));

        byte[] row = readText(in);
        byte[] cf = readText(in);
        byte[] cq = readText(in);
        byte[] cv = readText(in);

        long ts = WritableUtils.readVLong(in);
        // pass in copy=false to save double allocation of byte[]s
        key = new Key(row, cf, cq, cv, ts, in.readBoolean(), false);

        buildHashCode();
    }

    /* Read in byte[] to save Text object creation */
    private byte[] readText(DataInput in) throws IOException {
        byte[] data = new byte[WritableUtils.readVInt(in)];
        in.readFully(data, 0, data.length);
        return data;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        writeText(out, tableName);
        // reuse a text object for writing
        Text t = new Text();
        writeText(out, key.getRow(t));
        writeText(out, key.getColumnFamily(t));
        writeText(out, key.getColumnQualifier(t));
        writeText(out, key.getColumnVisibility(t));

        WritableUtils.writeVLong(out, key.getTimestamp());
        out.writeBoolean(key.isDeleted());
    }

    private void writeText(DataOutput out, Text t) throws IOException {
        WritableUtils.writeVInt(out, t.getLength());
        out.write(t.getBytes(), 0, t.getLength());
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this).append("tableName", tableName).append("key", key.toString()).toString();
    }

    /**
     * Set and override the table name.
     *
     * @param tableName
     *            the table name
     */
    public void setTableName(final Text tableName) {
        this.tableName.set(tableName);
        buildHashCode();
    }

    @Override
    public int compareTo(BulkIngestKey other) {
        int result = tableName.compareTo(other.tableName);
        if (result == 0) {
            result = key.compareTo(other.key);
        }
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
        BulkIngestKey other = (BulkIngestKey) obj;
        return compareTo(other) == 0;
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    /** A WritableComparator optimized for BulkIngestKey keys. */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(BulkIngestKey.class);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

            int o1 = s1;
            int o2 = s2;
            int[] startAndLen = {0, 0};
            // 5 parts to read (all Text... vint gives size of Text):
            // table name, row, col fam, col qual, col vis
            for (int i = 0; i < 5; i++) {
                startAndLen[0] = o1;
                // get Text's length in bytes
                int tl1 = readVInt(b1, startAndLen);
                o1 += startAndLen[1];
                startAndLen[0] = o2;
                int tl2 = readVInt(b2, startAndLen);
                o2 += startAndLen[1];

                int result = compareBytes(b1, o1, tl1, b2, o2, tl2);
                if (result != 0) {
                    return result;
                }
                o1 += tl1;
                o2 += tl2;
            }

            // get timestamps (vlong)
            startAndLen[0] = o1;
            long ts1 = readVLong(b1, startAndLen);
            o1 += startAndLen[1];
            startAndLen[0] = o2;
            long ts2 = readVLong(b2, startAndLen);
            o2 += startAndLen[1];

            if (ts1 < ts2) {
                return 1;
            } else if (ts1 > ts2) {
                return -1;
            }

            boolean deleted1 = readBoolean(b1, o1);
            boolean deleted2 = readBoolean(b2, o2);
            if (deleted1 != deleted2) {
                // if deleted=true return -1 indicating a deleted key is 'less than' a non-deleted key, and that
                // the deleted key must be sorted before the non-deleted key
                return (deleted1 ? -1 : 1);
            }

            return 0;
        }

        public static boolean readBoolean(byte[] bytes, int start) {
            return (bytes[start] != 0);
        }

        /**
         * Reads a Variable int from a byte[]
         *
         * @see Comparator#readVLong(byte[], int[])
         * @param bytes
         *            payload containing variable int
         * @param startAndLen
         *            index 0 holds the offset into the byte array and position 1 is populated with the length of the bytes
         * @return the value
         */
        public static int readVInt(byte[] bytes, int[] startAndLen) {
            return (int) readVLong(bytes, startAndLen);
        }

        /**
         * Reads a Variable Long from a byte[]. Also returns the variable int size in the second position (index 1) of the startAndLen array. This allows the
         * caller to have access to the VInt size without having to call decode again.
         *
         * @param bytes
         *            payload containing variable long
         * @param startAndLen
         *            index 0 holds the offset into the byte array and position 1 is populated with the length of the bytes
         * @return the value
         */
        public static long readVLong(byte[] bytes, int[] startAndLen) {
            byte firstByte = bytes[startAndLen[0]];
            startAndLen[1] = WritableUtils.decodeVIntSize(firstByte);
            if (startAndLen[1] == 1) {
                return firstByte;
            }
            long i = 0;
            for (int idx = 0; idx < startAndLen[1] - 1; idx++) {
                byte b = bytes[startAndLen[0] + 1 + idx];
                i = i << 8;
                i = i | (b & 0xFF);
            }
            return (WritableUtils.isNegativeVInt(firstByte) ? (i ^ -1L) : i);
        }
    }

    static {
        // register this comparator
        WritableComparator.define(BulkIngestKey.class, new Comparator());
    }
}
