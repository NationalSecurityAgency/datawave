package datawave.ingest.mapreduce.job;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;

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
 * The table name is serialized as a {@link BulkIndexKeyTableLookup} id when the job has published its output tables, and inline - a length prefix followed by
 * the name, as it has always been - when it has not. See {@link BulkIndexKeyTableLookup} for how the two forms interoperate and why the encoding leaves the
 * sort order of a fully declared job unchanged.
 *
 * <h2>The serialized layout</h2>
 *
 * <pre>
 * vint(tableId) [ vint(nameLen) nameBytes ]   the name only when tableId &lt; 0, i.e. the table is not in the dictionary
 * vint(rowLen) vint(cfLen) vint(cqLen) vint(cvLen)   the header: the four component lengths, together
 * rowBytes cfBytes cqBytes cvBytes                   the data region: component bytes back to back, no vints inside
 * vlong(timestamp) boolean(deleted)
 * </pre>
 *
 * The four length vints used to sit immediately ahead of the bytes they measure. Moving them together into a header carries exactly the same information in
 * exactly the same number of bytes - what it buys is a data region that {@link Comparator} can scan in one call rather than four, for the reasons that method's
 * javadoc sets out.
 * <p>
 * The layout is job-internal. It appears only in map output, spill files, and the shuffle, never in an RFile or an Accumulo table, and a job's writers and
 * readers are always the same build, so there is no compatibility to keep with any other form of it.
 */
public class BulkIngestKey implements WritableComparable<BulkIngestKey> {

    /** {@link #tableId} before the table name has been looked up in the installed dictionary */
    private static final int UNRESOLVED_ID = Integer.MIN_VALUE;

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
     * The id the installed {@link BulkIndexKeyTableLookup} gives {@link #tableName}, resolved and cached on first use. Keys arrive in runs on the same table,
     * and {@link BulkIndexKeyTableLookup#idFor(Text)} memoizes the last table it resolved, so the common resolution is one short {@code Text.equals} rather
     * than a hash probe over the name.
     *
     * @return the dictionary id, or {@link BulkIndexKeyTableLookup#UNKNOWN_ID} if the dictionary does not know this table
     */
    protected int getTableId() {
        if (tableId == UNRESOLVED_ID) {
            tableId = BulkIndexKeyTableLookup.get().idFor(tableName);
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
        /**
         * BulkIngestKey outKey = new BulkIngestKey(key.getTableName(), key.getKey()); outKey.getKey().setTimestamp(-1 * ts); // mutates the Key AFTER the hash
         * is built
         *
         * On integration, outKey's hash reflects the pre-mutation timestamp. Two events on the same day produce outKeys that are equal by compareTo but have
         * different hashCodes — a latent equals/hashCode violation that happens to be harmless because HashMultimap then keeps them in separate buckets and the
         * reducer dedupes downstream. If the constructor were lazy, the hash would be computed post-mutation at cache.put, the equal keys would collide, and
         * HashMultimap's set semantics would start deduping (key,value) pairs in the mapper — a real behavior change. Keeping the constructor eager preserves
         * the integration behavior bit-for-bit. So: hashcode does not manipulate the dedupe path, and the constructor is precisely why.
         */
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
     * Like {@link #getTableId()}, this trusts that the id a table has is stable for the life of the JVM, which {@link BulkIndexKeyTableLookup} guarantees by
     * installing once, before the first record is read.
     * <p>
     * The four component lengths arrive together, ahead of the bytes they measure - see the class javadoc for the layout - so all four are decoded before the
     * first component array is allocated.
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        int id = WritableUtils.readVInt(in);
        if (id < 0) {
            tableName = new Text(readText(in));
        } else if (id != tableId) {
            Text name = BulkIndexKeyTableLookup.get().nameFor(id);
            if (null == name) {
                throw new IOException("Table id " + id + " is not in this JVM's table name dictionary, which holds " + BulkIndexKeyTableLookup.get().size()
                                + " tables. The writer and the reader of this record disagree on " + TableConfigurationUtil.JOB_OUTPUT_TABLE_NAMES + ".");
            }
            // copy: the dictionary's Text is shared with every other key on this table, and setTableName writes through
            tableName = new Text(name);
        }
        // adopt the id the record was written with rather than re-resolving, so the object ordering cannot drift from the serialized ordering
        tableId = id;

        // the header, then the data region it describes
        int rowLen = WritableUtils.readVInt(in);
        int cfLen = WritableUtils.readVInt(in);
        int cqLen = WritableUtils.readVInt(in);
        int cvLen = WritableUtils.readVInt(in);

        byte[] row = readBytes(in, rowLen);
        byte[] cf = readBytes(in, cfLen);
        byte[] cq = readBytes(in, cqLen);
        byte[] cv = readBytes(in, cvLen);

        long ts = WritableUtils.readVLong(in);
        // pass in copy=false to save double allocation of byte[]s
        key = new Key(row, cf, cq, cv, ts, in.readBoolean(), false);

        hashCode = UNCOMPUTED_HASH;
    }

    /* Read a length-prefixed byte[] - the inline table name's form - to save Text object creation */
    private byte[] readText(DataInput in) throws IOException {
        return readBytes(in, WritableUtils.readVInt(in));
    }

    /* Read a component of the length its header entry gave */
    private byte[] readBytes(DataInput in, int length) throws IOException {
        byte[] data = new byte[length];
        in.readFully(data, 0, length);
        return data;
    }

    /**
     * {@inheritDoc}
     * <p>
     * Writes the table id (and, for a table the dictionary does not know, the name inline), then the four component lengths as a header, then the four
     * components' bytes back to back, then the timestamp and the deleted flag. The class javadoc has the layout and why the lengths are grouped.
     */
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
        ByteSequence row = key.getRowData();
        ByteSequence cf = key.getColumnFamilyData();
        ByteSequence cq = key.getColumnQualifierData();
        ByteSequence cv = key.getColumnVisibilityData();

        // the header: all four lengths, ahead of all four components
        WritableUtils.writeVInt(out, row.length());
        WritableUtils.writeVInt(out, cf.length());
        WritableUtils.writeVInt(out, cq.length());
        WritableUtils.writeVInt(out, cv.length());

        writeByteSequence(out, row);
        writeByteSequence(out, cf);
        writeByteSequence(out, cq);
        writeByteSequence(out, cv);

        WritableUtils.writeVLong(out, key.getTimestamp());
        out.writeBoolean(key.isDeleted());
    }

    private void writeText(DataOutput out, Text t) throws IOException {
        WritableUtils.writeVInt(out, t.getLength());
        out.write(t.getBytes(), 0, t.getLength());
    }

    private void writeByteSequence(DataOutput out, ByteSequence bs) throws IOException {
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
        int result = BulkIndexKeyTableLookup.compareIds(thisId, other.getTableId());
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
         * Installs the job's {@link BulkIndexKeyTableLookup}. MapReduce resolves the map output key comparator through
         * {@code WritableComparator.get(Class, Configuration)} while it builds the map output collector and again while it builds the reduce side merge, so
         * this runs in both JVMs and, in both, before any {@link BulkIngestKey} is serialized or deserialized.
         *
         * @param conf
         *            the job configuration
         */
        @Override
        public void setConf(Configuration conf) {
            super.setConf(conf);
            BulkIndexKeyTableLookup.configure(conf);
        }

        /**
         * {@inheritDoc}
         * <p>
         * After the table component, this decodes both records' four component lengths - which the layout groups into a header ahead of the data, see the
         * {@link BulkIngestKey} class javadoc - and settles the four components with a <em>single</em>
         * {@link Arrays#mismatch(byte[], int, int, byte[], int, int)} over the data region rather than a comparison per component.
         *
         * <h2>Why the header makes one scan enough</h2>
         *
         * The old interleaved layout could not be scanned this way, because a length vint sat between every pair of components: the vints are part of the bytes
         * being scanned, they do not order the components they measure, and a first difference landing inside one meant the scan had learned nothing usable.
         * With the lengths hoisted out, the data region is nothing but component bytes, and the header is read before the scan rather than during it. That
         * turns the two cases the header can present into two straightforward ones:
         *
         * <ul>
         * <li><strong>the headers agree through component {@code d-1}</strong> - then components {@code 0..d-1} occupy the same offsets, relative to each
         * record's data start, in both records. The whole {@code alignedLen} byte prefix of the data region is therefore alignment-safe, and one scan over it
         * finds the first byte the two records disagree on. Because both ranges are the same length and the byte at index {@code k} belongs to the same
         * component at the same intra-component offset in both, its unsigned difference is exactly what a per component {@code compareBytes} would have
         * returned, and every earlier component was equal - so it is the answer.</li>
         * <li><strong>the headers differ at component {@code d}</strong> - then the header has said, before a single data byte is read, precisely where
         * alignment breaks. Components after {@code d} are never examined, because component {@code d} <em>must</em> decide: its two contents differ in length,
         * so either they differ in some byte, or one is a proper prefix of the other and {@code compareBytes} returns the nonzero length difference.</li>
         * </ul>
         *
         * Only when all four lengths agree and the entire data region matches does anything past it get decoded, and then the timestamp and deleted flag are
         * compared by value exactly as they always were.
         *
         * @param b1
         *            the buffer holding the left record
         * @param s1
         *            the offset of the left record
         * @param l1
         *            the serialized length of the left record
         * @param b2
         *            the buffer holding the right record
         * @param s2
         *            the offset of the right record
         * @param l2
         *            the serialized length of the right record
         * @return a negative number, zero, or a positive number as the left record sorts before, with, or after the right
         */
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

            int idResult = BulkIndexKeyTableLookup.compareIds(id1, id2);
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

            // the header: the lengths of row, col fam, col qual and col vis, in that order, for each record.
            // writeVInt stores any value in 0..127 as the byte itself, and every component sharded ingest emits
            // is shorter than that - so the common header is four single-byte entries, recognized in one test by
            // all four bytes being non-negative at once, and read without the general decoder or its int[] traffic
            int rowLen1;
            int cfLen1;
            int cqLen1;
            int cvLen1;
            if ((b1[o1] | b1[o1 + 1] | b1[o1 + 2] | b1[o1 + 3]) >= 0) {
                rowLen1 = b1[o1];
                cfLen1 = b1[o1 + 1];
                cqLen1 = b1[o1 + 2];
                cvLen1 = b1[o1 + 3];
                o1 += 4;
            } else {
                // at least one component is 128 bytes or longer, so its entry needs the multi byte vint form
                startAndLen[0] = o1;
                rowLen1 = readLength(b1, startAndLen);
                o1 += startAndLen[1];
                startAndLen[0] = o1;
                cfLen1 = readLength(b1, startAndLen);
                o1 += startAndLen[1];
                startAndLen[0] = o1;
                cqLen1 = readLength(b1, startAndLen);
                o1 += startAndLen[1];
                startAndLen[0] = o1;
                cvLen1 = readLength(b1, startAndLen);
                o1 += startAndLen[1];
            }

            int rowLen2;
            int cfLen2;
            int cqLen2;
            int cvLen2;
            if ((b2[o2] | b2[o2 + 1] | b2[o2 + 2] | b2[o2 + 3]) >= 0) {
                rowLen2 = b2[o2];
                cfLen2 = b2[o2 + 1];
                cqLen2 = b2[o2 + 2];
                cvLen2 = b2[o2 + 3];
                o2 += 4;
            } else {
                startAndLen[0] = o2;
                rowLen2 = readLength(b2, startAndLen);
                o2 += startAndLen[1];
                startAndLen[0] = o2;
                cfLen2 = readLength(b2, startAndLen);
                o2 += startAndLen[1];
                startAndLen[0] = o2;
                cqLen2 = readLength(b2, startAndLen);
                o2 += startAndLen[1];
                startAndLen[0] = o2;
                cvLen2 = readLength(b2, startAndLen);
                o2 += startAndLen[1];
            }

            // the data regions start where the headers end
            int data1 = o1;
            int data2 = o2;

            // alignedLen is the number of leading data bytes the two records lay out identically - which is what
            // makes one scan of them sound - and dl1/dl2 are the two lengths of the first component whose lengths
            // differ, the component that must decide when the aligned prefix ties. dl1 stays -1 if all four agree.
            int alignedLen;
            int dl1 = -1;
            int dl2 = -1;
            if (rowLen1 != rowLen2) {
                alignedLen = 0;
                dl1 = rowLen1;
                dl2 = rowLen2;
            } else if (cfLen1 != cfLen2) {
                alignedLen = rowLen1;
                dl1 = cfLen1;
                dl2 = cfLen2;
            } else if (cqLen1 != cqLen2) {
                alignedLen = rowLen1 + cfLen1;
                dl1 = cqLen1;
                dl2 = cqLen2;
            } else if (cvLen1 != cvLen2) {
                alignedLen = rowLen1 + cfLen1 + cqLen1;
                dl1 = cvLen1;
                dl2 = cvLen2;
            } else {
                alignedLen = rowLen1 + cfLen1 + cqLen1 + cvLen1;
            }

            // one scan over every byte the two records place at the same offset. Both ranges are alignedLen long, so
            // mismatch cannot report the one-is-a-prefix-of-the-other outcome - a non-negative k is a genuine
            // differing byte, at the same offset within the same component of both records.
            int k = Arrays.mismatch(b1, data1, data1 + alignedLen, b2, data2, data2 + alignedLen);
            if (k >= 0) {
                return (b1[data1 + k] & 0xff) - (b2[data2 + k] & 0xff);
            }

            if (dl1 >= 0) {
                // the aligned prefix matched, so every component ahead of the length-differing one is equal and that
                // component decides. It cannot tie: its two lengths differ, so either some byte differs, or one
                // content is a proper prefix of the other and compareBytes returns the length difference. Nothing
                // after it is ever looked at.
                return compareBytes(b1, data1 + alignedLen, dl1, b2, data2 + alignedLen, dl2);
            }

            // all four components are equal; alignedLen is the whole data region, so the timestamps follow it

            // get timestamps (vlong)
            o1 = data1 + alignedLen;
            o2 = data2 + alignedLen;
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

        /**
         * Reads one header entry - a component length, which is never negative. Only called on the header fallback path, when some component in the record is
         * 128 bytes or longer: the other entries of such a header are usually still the single byte {@code 0..127} form, so this keeps that case to one array
         * read and consults the general decoder only for the entries that really are multi byte.
         *
         * @see Comparator#readVLong(byte[], int[])
         * @param bytes
         *            payload containing the header
         * @param startAndLen
         *            index 0 holds the offset into the byte array and position 1 is populated with the length of the bytes
         * @return the component length
         */
        private static int readLength(byte[] bytes, int[] startAndLen) {
            byte first = bytes[startAndLen[0]];
            if (first >= 0) {
                startAndLen[1] = 1;
                return first;
            }
            return readVInt(bytes, startAndLen);
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
