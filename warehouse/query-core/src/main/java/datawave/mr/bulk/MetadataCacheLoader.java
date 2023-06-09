package datawave.mr.bulk;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.Sets;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import datawave.query.util.Tuple2;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.RowIterator;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.TableId;
import org.apache.accumulo.core.dataImpl.KeyExtent;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.metadata.MetadataTable;
import org.apache.accumulo.core.metadata.schema.MetadataSchema;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;

/**
 * A cache loader that maps accumulo metadata ranges to the files and locations.
 *
 * NOTE: It is assumed that the metadata range was one produced by the createMetadataRange method below, or at least we have an inclusive end key.
 *
 */
public class MetadataCacheLoader extends CacheLoader<Range,Set<Tuple2<String,Set<String>>>> {

    private static final Logger log = Logger.getLogger(MetadataCacheLoader.class);
    protected AccumuloClient client;
    protected String defaultBasePath;

    private static final String HDFS_BASE = "hdfs://";

    public MetadataCacheLoader(AccumuloClient client, String defaultBasePath) {
        this.client = client;
        this.defaultBasePath = defaultBasePath;
    }

    @Override
    public Set<Tuple2<String,Set<String>>> load(Range inputKey) throws Exception {

        Set<Tuple2<String,Set<String>>> locations = new HashSet<>();

        // determine the table id
        final String metadataString = inputKey.getStartKey().getRow().toString().intern();
        final TableId tableId = TableId.of(getTableId(metadataString));

        // determine our stop criteria
        final String stopRow = inputKey.getEndKey().getRow().toString().intern();

        // create a scan range that goes through the default tablet
        Key endKey = new Key(new KeyExtent(tableId, null, null).toMetaRow()).followingKey(PartialKey.ROW);
        Range metadataRange = new Range(inputKey.getStartKey(), inputKey.isStartKeyInclusive(), endKey, false);

        Scanner scanner = client.createScanner(MetadataTable.NAME, Authorizations.EMPTY);
        MetadataSchema.TabletsSection.TabletColumnFamily.PREV_ROW_COLUMN.fetch(scanner);
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.LastLocationColumnFamily.NAME);
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME);
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME);
        scanner.fetchColumnFamily(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME);
        scanner.setRange(metadataRange);

        RowIterator rowIter = new RowIterator(scanner);

        String baseLocation = defaultBasePath + MultiRfileInputformat.tableStr + tableId + Path.SEPARATOR;
        try {

            while (rowIter.hasNext()) {

                Iterator<Entry<Key,Value>> row = rowIter.next();
                String location = "";
                String endRow = "";
                Set<String> fileLocations = Sets.newHashSet();

                while (row.hasNext()) {
                    Entry<Key,Value> entry = row.next();
                    Key key = entry.getKey();
                    endRow = key.getRow().toString().intern();

                    if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.DataFileColumnFamily.NAME)) {
                        String fileLocation = entry.getKey().getColumnQualifier().toString();
                        if (!fileLocation.contains(HDFS_BASE))
                            fileLocation = baseLocation.concat(entry.getKey().getColumnQualifier().toString());
                        fileLocations.add(fileLocation);
                    }

                    if (key.getColumnFamily().equals(MetadataSchema.TabletsSection.CurrentLocationColumnFamily.NAME)
                                    || key.getColumnFamily().equals(MetadataSchema.TabletsSection.FutureLocationColumnFamily.NAME)) {
                        location = entry.getValue().toString();
                    }

                }

                locations.add(new Tuple2<>(location, fileLocations));

                // if this row is equal to or past our stop row, then terminate
                if (endRow.compareTo(stopRow) >= 0) {
                    break;
                }
            }

        } finally {
            scanner.close();
        }

        return locations;
    }

    /**
     * Pull the table id off of the beginning of a metadata table row
     *
     * @param row
     *            the row
     * @return the tableId
     */
    public static String getTableId(String row) {
        // pull the characters up to the first ; or <
        for (int i = 1; i < row.length(); i++) {
            char c = row.charAt(i);
            if (c == ';' || c == '<') {
                return row.substring(0, i);
            }
        }
        return row;
    }

    /**
     * Convert a range into the required range against the metadata table
     *
     * @param tableId
     *            The table id for the table we are scanning
     * @param range
     *            The range against the table
     * @return the accumulo metadata table range
     */
    public static Range createMetadataRange(TableId tableId, Range range) {
        Text startRow;
        if (range.getStartKey() != null) {
            startRow = range.getStartKey().getRow();
        } else {
            startRow = new Text(); // setting to an empty text will result in a start key of <tableId>;
        }
        Key startKey = new Key(new KeyExtent(tableId, startRow, null).toMetaRow());

        Text endRow;
        if (range.getEndKey() != null) {
            endRow = range.getEndKey().getRow();
            // if we have a non-inclusive end key with a null byte at the end, then strip the null byte
            // such that we can create an inclusive range.
            if (!range.isEndKeyInclusive() && endRow.getBytes()[endRow.getLength() - 1] == 0) {
                byte[] endRowBytes = new byte[endRow.getLength() - 1];
                System.arraycopy(endRow.getBytes(), 0, endRowBytes, 0, endRowBytes.length);
                endRow = new Text(endRowBytes);
            }
        } else {
            endRow = null; // setting to null will result in a end key of <tableId><
        }
        Key endKey = new Key(new KeyExtent(tableId, endRow, null).toMetaRow());
        Range metadataRange = new Range(startKey, true, endKey, true);
        if (log.isDebugEnabled()) {
            log.debug("Converted " + range + " into metadata range " + metadataRange);
        }
        return metadataRange;
    }
}
