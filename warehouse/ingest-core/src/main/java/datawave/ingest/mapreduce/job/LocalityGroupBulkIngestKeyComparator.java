package datawave.ingest.mapreduce.job;

import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

public class LocalityGroupBulkIngestKeyComparator extends WritableComparator implements Configurable {
    private final static int IDX_TABLE = 0;
    private final static int IDX_COLF = 2;

    private final Text tableHolder;
    private final Text tablePreviousHolder;
    private final Text colfHolder;

    private Configuration conf;
    private LocalityGroupSupport lgSupport;
    private LocalityGroupConfiguration lgConf;
    private LocalityGroupSupport.ColumnFamilyToLocalityGroup colfLg;

    public LocalityGroupBulkIngestKeyComparator() {
        super(BulkIngestKey.class);
        this.tableHolder = new Text();
        this.tablePreviousHolder = new Text();
        this.colfHolder = new Text();
        this.lgSupport = LocalityGroupSupport.emptyLocalityGroupSupport();
    }

    @Override
    public Configuration getConf() {
        return conf;
    }

    @Override
    public void setConf(Configuration conf) {
        this.conf = conf;
        try {
            this.lgSupport = LocalityGroupSupport.builder()
                .withLocalityGroupConfiguration(TableConfigurationUtil.getJobOutputLocalityGroupConfiguration(conf))
                .build();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
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

            if (result != 0 && i == IDX_TABLE) {
                colfLg = null;
                tablePreviousHolder.clear();
            } else if (result == 0 && i == IDX_TABLE) {
                tableHolder.set(b1, o1, tl1);
            } else if (result != 0 && i == IDX_COLF) {
                if (!tablePreviousHolder.equals(tableHolder)) {
                    tablePreviousHolder.set(tableHolder);
                    colfLg = lgSupport.getColumnFamilyToLocalityGroup(tableHolder);

                    // no colf/lg mapping - this can mean the table is not configured
                    // and may have been excluded on purpose
                    // assume default lg and create an empty mapping
                    if (colfLg == null) {
                        colfLg = new LocalityGroupSupport.ColumnFamilyToLocalityGroup(Map.of());
                    }
                }
                String lg1 = computeColumnFamilyLocalityGroup(b1, o1, tl1);
                String lg2 = computeColumnFamilyLocalityGroup(b2, o2, tl2);
                if (lg1 != null && lg2 != null) {
                    result = lg1.compareTo(lg2);
                } else if (lg1 != null) {
                    // ensure locality-group sorts before default
                    // lg1 <lg-name>
                    // lg2 <default>
                    // lg1 < lg2
                    result = -1;
                } else if (lg2 != null) {
                    // ensure default-locality sorts after default
                    // lg1 <default>
                    // lg2 <lg-name>
                    // lg1 > lg2
                    result = 1;
                }
            }

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

    @VisibleForTesting
    void setLocalityGroupConfiguration(LocalityGroupConfiguration lgConf) {
        this.lgConf = lgConf;
    }

    private @Nullable String computeColumnFamilyLocalityGroup(byte[] buffer, int offset, int len) {
        String lg = null;
        if (!colfLg.isEmpty() && len <= colfLg.columnFamilyMaxLength() && len >= colfLg.columnFamilyMinLength()) {
            colfHolder.set(buffer, offset, len);
            lg = colfLg.columnFamilyToLocalityGroup().get(colfHolder);
        }
        return lg;
    }

    public static boolean readBoolean(byte[] bytes, int start) {
        return (bytes[start] != 0);
    }

    /**
     * Reads a Variable int from a byte[]
     *
     * @see BulkIngestKey.Comparator#readVLong(byte[], int[])
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
