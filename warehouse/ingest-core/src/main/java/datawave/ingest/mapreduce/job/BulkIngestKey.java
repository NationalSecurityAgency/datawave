package datawave.ingest.mapreduce.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.commons.lang.builder.ToStringBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

/**
 * Used during bulk ingest to convey the table name to the reducer and stores the key for sorting.
 * <p>
 * The table name is serialized as a {@link TableNameDictionary} id when the job has published its output tables, and inline - a length prefix followed by the
 * name, as it has always been - when it has not. See {@link TableNameDictionary} for how the two forms interoperate and why the encoding leaves the sort order
 * of a fully declared job unchanged.
 */
public class BulkIngestKey implements WritableComparable<BulkIngestKey> {

    /** {@link #tableId} before the table name has been looked up in the installed dictionary */
    private static final int UNRESOLVED_ID = Integer.MIN_VALUE;

    /**
     * The table id {@link #getTableId()} resolved most recently, kept as a one entry memo across keys.
     * <p>
     * Keys arrive in runs on the same table - a handler emits an event key, then its field index entries, then its global index entries - so most lookups ask
     * for the table the previous key asked for. Skipping the hash lookup on those is worth it because the lookup is a {@link Text} probe: {@code Text.hashCode}
     * is not memoized, so it walks every byte of the name to hash it and then walks them again in {@code equals}. The memo replaces that with a single
     * {@code Text.equals} against the name the id stands for, which rejects a different table on the length comparison it starts with.
     * <p>
     * Only the id is stored, and it is validated against {@link TableNameDictionary#nameFor(int)} before being trusted. That is what lets this be a plain
     * {@code int} with no synchronization: an {@code int} write cannot tear, so every value this field can hold is either rejected by the equality check or is
     * genuinely the id of the name being resolved. A race between threads costs a missed memo, never a wrong id, and a dictionary swap invalidates the memo for
     * free because the validation reads the names of whichever dictionary is now installed. Caching the name alongside the id in a second field would not be
     * safe this way - a reader could pair one thread's name with another thread's id.
     */
    private static int lastResolvedTableId = 0;

    /**
     * {@link #hashCode} before it has been computed. A key whose hash genuinely is zero simply recomputes it on every call, which costs a little and returns
     * the same answer.
     */
    private static final int UNCOMPUTED_HASH = 0;

    protected Text tableName = null;

    /**
     * Deliberately not initialized here: the 2-arg constructor and {@link #readFields(DataInput)} both replace the field immediately, so a field initializer
     * would allocate a {@link Key} per mapper-emitted record just to discard it. The no-arg constructor supplies the default instead.
     */
    protected Key key;

    // computed hashcode. we won't write this through the writable interface
    // to avoid increasing the size of our spilled data
    protected int hashCode = UNCOMPUTED_HASH;

    /**
     * The dictionary id of {@link #tableName}, resolved on first use rather than at construction: a mapper builds far more of these than it serializes or
     * compares, and the ones a dedupe writer collapses never need an id at all.
     */
    protected int tableId = UNRESOLVED_ID;

    public BulkIngestKey() {
        this.tableName = new Text();
        this.key = new Key();
        // eager, see buildHashCode()
        buildHashCode();
    }

    public BulkIngestKey(Text tableName, Key key) {
        super();
        this.tableName = tableName;
        if (null == this.tableName) {
            this.tableName = new Text();
        }
        this.key = key;
        // eager, see buildHashCode()
        buildHashCode();
    }

    public Text getTableName() {
        return tableName;
    }

    public Key getKey() {
        return key;
    }

    /**
     * The id the installed {@link TableNameDictionary} gives {@link #tableName}, resolved and cached on first use. The hash lookup is skipped entirely when
     * this key is on the same table as the last one to ask - see {@link #lastResolvedTableId}.
     *
     * @return the dictionary id, or {@link TableNameDictionary#UNKNOWN_ID} if the dictionary does not know this table
     */
    protected int getTableId() {
        if (tableId == UNRESOLVED_ID) {
            TableNameDictionary dictionary = TableNameDictionary.get();

            // read the memo once into a local, so the name it is validated against is the name of the id we return
            int last = lastResolvedTableId;
            Text lastName = dictionary.nameFor(last);
            if (null != lastName && lastName.equals(tableName)) {
                tableId = last;
            } else {
                tableId = dictionary.idFor(tableName);
                // an unknown table has no id to remember, and leaving the memo alone keeps it useful for the keys
                // on either side of it
                if (tableId != TableNameDictionary.UNKNOWN_ID) {
                    lastResolvedTableId = tableId;
                }
            }
        }
        return tableId;
    }

    /**
     * Build the computed hash code.
     * <p>
     * {@link #hashCode()} computes this on demand, so the mutators that used to call this - {@link #readFields(DataInput)} and {@link #setTableName(Text)} -
     * only mark it {@link #UNCOMPUTED_HASH} now. Most keys a mapper builds are never hashed: they are written, compared, or collapsed by a dedupe writer that
     * uses {@code compareTo}, and hashing costs a walk over the whole table name plus the {@link Key}'s five components.
     * <p>
     * The constructors still build it eagerly, for backwards compatibility with anything reading the protected {@link #hashCode} field directly rather than
     * calling {@link #hashCode()}. That is temporary - once the field is private, the constructors can leave it {@link #UNCOMPUTED_HASH} like the mutators do.
     */
    protected void buildHashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((key == null) ? 0 : key.hashCode());
        result = prime * result + ((tableName == null) ? 0 : tableName.hashCode());
        hashCode = result;
    }

    /**
     * {@inheritDoc}
     * <p>
     * The deserializer reads records into one reused key object, and records arrive grouped by table, so {@link #tableId} and {@link #tableName} already hold
     * the answer for all but the first record of each run - {@code tableId} is only ever non-negative when {@code tableName} is the name the installed
     * dictionary gives that id. Reading the same id again therefore skips resolving and copying the name, which is the only allocation the table component
     * costs. Two records read back to back into the same object then share one {@code tableName} instance; nothing mutates it, since a run ending allocates a
     * fresh {@link Text} rather than overwriting the old one.
     * <p>
     * Like {@link #getTableId()}, this trusts that the id a table has is stable for the life of the JVM, which {@link TableNameDictionary} guarantees by
     * installing once, before the first record is read.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        int id = WritableUtils.readVInt(in);
        if (id < 0) {
            tableName = new Text(readText(in));
        } else if (id != tableId) {
            Text name = TableNameDictionary.get().nameFor(id);
            if (null == name) {
                throw new IOException("Table id " + id + " is not in this JVM's table name dictionary, which holds " + TableNameDictionary.get().size()
                                + " tables. The writer and the reader of this record disagree on " + TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES + ".");
            }
            // copy: the dictionary's Text is shared with every other key on this table, and setTableName writes through
            tableName = new Text(name);
        }
        // adopt the id the record was written with rather than re-resolving, so the object ordering cannot drift from the serialized ordering
        tableId = id;

        byte[] row = readText(in);
        byte[] cf = readText(in);
        byte[] cq = readText(in);
        byte[] cv = readText(in);

        long ts = WritableUtils.readVLong(in);
        // pass in copy=false to save double allocation of byte[]s
        key = new Key(row, cf, cq, cv, ts, in.readBoolean(), false);

        hashCode = UNCOMPUTED_HASH;
    }

    /* Read in byte[] to save Text object creation */
    private byte[] readText(DataInput in) throws IOException {
        byte[] data = new byte[WritableUtils.readVInt(in)];
        in.readFully(data, 0, data.length);
        return data;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        int id = getTableId();
        WritableUtils.writeVInt(out, id);
        if (id < 0) {
            // not a table this job declared: fall back to the name, which every reader still understands
            writeText(out, tableName);
        }

        // write each component straight from the Key's backing arrays: getRow(Text) and friends would copy
        // every component into a scratch Text first just to hand write() the same bytes
        writeByteSequence(out, key.getRowData());
        writeByteSequence(out, key.getColumnFamilyData());
        writeByteSequence(out, key.getColumnQualifierData());
        writeByteSequence(out, key.getColumnVisibilityData());

        WritableUtils.writeVLong(out, key.getTimestamp());
        out.writeBoolean(key.isDeleted());
    }

    private void writeText(DataOutput out, Text t) throws IOException {
        WritableUtils.writeVInt(out, t.getLength());
        out.write(t.getBytes(), 0, t.getLength());
    }

    private void writeByteSequence(DataOutput out, ByteSequence bs) throws IOException {
        WritableUtils.writeVInt(out, bs.length());
        if (bs.isBackedByArray()) {
            out.write(bs.getBackingArray(), bs.offset(), bs.length());
        } else {
            // no Key produces an array-less ByteSequence today, but the interface permits one
            out.write(bs.toArray());
        }
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
        this.tableId = UNRESOLVED_ID;
        this.hashCode = UNCOMPUTED_HASH;
    }

    @Override
    public int compareTo(BulkIngestKey other) {
        int thisId = getTableId();
        int result = TableNameDictionary.compareIds(thisId, other.getTableId());
        if (result == 0 && thisId < 0) {
            // neither table is in the dictionary, which is the one case the ids do not separate
            result = tableName.compareTo(other.tableName);
        }
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
        if (hashCode == UNCOMPUTED_HASH) {
            buildHashCode();
        }
        return hashCode;
    }

    /** A WritableComparator optimized for BulkIngestKey keys. */
    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(BulkIngestKey.class);
        }

        /**
         * Installs the job's {@link TableNameDictionary}. MapReduce resolves the map output key comparator through
         * {@code WritableComparator.get(Class, Configuration)} while it builds the map output collector and again while it builds the reduce side merge, so
         * this runs in both JVMs and, in both, before any {@link BulkIngestKey} is serialized or deserialized.
         *
         * @param conf
         *            the job configuration
         */
        @Override
        public void setConf(Configuration conf) {
            super.setConf(conf);
            TableNameDictionary.configure(conf);
        }

        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {

            int o1 = s1;
            int o2 = s2;
            int[] startAndLen = {0, 0};

            // the table, as a dictionary id. write() only ever emits ids >= -1, and every one of those occupies
            // the single byte form of the vint encoding, which stores the value as the byte itself - so the common
            // decode is one array read. A first byte below -1 is a multi byte length marker, or a single byte value
            // no writer produces; either way the general decoder handles it.
            int id1;
            byte first1 = b1[o1];
            if (first1 >= -1) {
                id1 = first1;
                o1++;
            } else {
                startAndLen[0] = o1;
                id1 = readVInt(b1, startAndLen);
                o1 += startAndLen[1];
            }
            int id2;
            byte first2 = b2[o2];
            if (first2 >= -1) {
                id2 = first2;
                o2++;
            } else {
                startAndLen[0] = o2;
                id2 = readVInt(b2, startAndLen);
                o2 += startAndLen[1];
            }

            int idResult = TableNameDictionary.compareIds(id1, id2);
            if (idResult != 0) {
                return idResult;
            }

            if (id1 < 0) {
                // neither table is in the dictionary, so both records carry their name inline and the tie breaks on the names
                startAndLen[0] = o1;
                int nl1 = readVInt(b1, startAndLen);
                o1 += startAndLen[1];
                startAndLen[0] = o2;
                int nl2 = readVInt(b2, startAndLen);
                o2 += startAndLen[1];

                int nameResult = compareBytes(b1, o1, nl1, b2, o2, nl2);
                if (nameResult != 0) {
                    return nameResult;
                }
                o1 += nl1;
                o2 += nl2;
            }

            // 4 parts to read (all Text... vint gives size of Text):
            // row, col fam, col qual, col vis
            for (int i = 0; i < 4; i++) {
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
