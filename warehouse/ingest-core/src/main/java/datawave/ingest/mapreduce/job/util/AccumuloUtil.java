package datawave.ingest.mapreduce.job.util;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.scan.ScannerBuilder;

public class AccumuloUtil {
    private static final String DEFAULT_METADATA_TABLE = "accumulo.metadata";

    private AccumuloClient accumuloClient;

    public AccumuloUtil() {}

    public void setAccumuloClient(AccumuloClient accumuloClient) {
        this.accumuloClient = accumuloClient;
    }

    public void setup(Configuration config, String clientPropertiesPath, String instanceName, String zookeepers, String username, String password) {
        accumuloClient = setupAccumuloClient(config, clientPropertiesPath, instanceName, zookeepers, username, password);
    }

    public List<Map.Entry<String,List<String>>> getFilesFromMetadataBySplit(String tableName, String startRow, String endRow) throws AccumuloException {
        if (accumuloClient == null) {
            throw new IllegalStateException("Must call setup() before use");
        }

        return getFilesFromMetadataBySplit(accumuloClient, tableName, startRow, endRow);
    }

    public List<Map.Entry<String,List<String>>> getFilesFromMetadataBySplit(String tableName, String startRow, String endRow, boolean allowFencedFiles)
                    throws AccumuloException {
        if (accumuloClient == null) {
            throw new IllegalStateException("Must call setup() before use");
        }

        return getFilesFromMetadataBySplit(accumuloClient, DEFAULT_METADATA_TABLE, tableName, startRow, endRow, allowFencedFiles);
    }

    public List<Map.Entry<String,List<String>>> getFilesFromMetadataBySplit(String accumuloMetadataTable, String tableName, String startRow, String endRow)
                    throws AccumuloException {
        if (accumuloClient == null) {
            throw new IllegalStateException("Must call setup() before use");
        }

        return getFilesFromMetadataBySplit(accumuloClient, accumuloMetadataTable, tableName, startRow, endRow);
    }

    public void close() {
        if (accumuloClient != null) {
            accumuloClient.close();
            accumuloClient = null;
        }
    }

    public static AccumuloClient setupAccumuloClient(Configuration config, String clientPropertiesPath, String instanceName, String zookeepers, String username,
                    String password) {
        AccumuloHelper.setInstanceName(config, instanceName);
        AccumuloHelper.setZooKeepers(config, zookeepers);
        AccumuloHelper.setUsername(config, username);
        AccumuloHelper.setPassword(config, password.getBytes());
        if (clientPropertiesPath != null) {
            AccumuloHelper.setClientPropertiesPath(config, clientPropertiesPath);
        }

        AccumuloHelper helper = new AccumuloHelper();
        helper.setup(config);

        return helper.newClient();
    }

    public static List<Map.Entry<String,List<String>>> getFilesFromMetadataBySplit(AccumuloClient accumuloClient, String tableName, String startRow,
                    String endRow) throws AccumuloException {
        return getFilesFromMetadataBySplit(accumuloClient, DEFAULT_METADATA_TABLE, tableName, startRow, endRow);
    }

    public static List<Map.Entry<String,List<String>>> getFilesFromMetadataBySplit(AccumuloClient accumuloClient, String accumuloMetadataTable,
                    String tableName, String startRow, String endRow) throws AccumuloException {
        return getFilesFromMetadataBySplit(accumuloClient, accumuloMetadataTable, tableName, startRow, endRow, false);
    }

    /**
     * Gets the paths of the files of each tablet of a table between two rows, from the metadata table.
     *
     * @param accumuloClient
     *            the client
     * @param accumuloMetadataTable
     *            the metadata table
     * @param tableName
     *            the table whose files to get
     * @param startRow
     *            the first row, or null for the start of the table
     * @param endRow
     *            the last row, or null for the end of the table
     * @param allowFencedFiles
     *            whether to return files that a tablet only reads part of. Reading such a file directly reads rows outside the tablet, so only allow them when
     *            the files are not read as the tablet's data
     * @return each tablet's end row and the paths of its files
     * @throws AccumuloException
     *             if either table does not exist
     * @throws IllegalStateException
     *             if a file is fenced and allowFencedFiles is false
     */
    public static List<Map.Entry<String,List<String>>> getFilesFromMetadataBySplit(AccumuloClient accumuloClient, String accumuloMetadataTable,
                    String tableName, String startRow, String endRow, boolean allowFencedFiles) throws AccumuloException {
        List<Map.Entry<String,List<String>>> metadataFiles = new ArrayList<>();

        String tableId = accumuloClient.tableOperations().tableIdMap().get(tableName);

        if (tableId == null) {
            throw new AccumuloException("Could not locate table: '" + tableName + "'");
        }

        String accumuloMetadataTableId = accumuloClient.tableOperations().tableIdMap().get(accumuloMetadataTable);
        if (accumuloMetadataTableId == null) {
            throw new AccumuloException("Could not locate table: '" + accumuloMetadataTable + "'");
        }

        // startRow becomes nothing
        if (startRow == null) {
            startRow = "";
        }

        // endRow becomes the last possible character in the table
        if (endRow == null) {
            endRow = "<" + '\u0000';
        } else if (endRow.equals(startRow)) {
            endRow = ";" + startRow + '\u0000';
        } else {
            endRow = ";" + endRow;
        }

        Text currentRow = null;
        List<String> files = null;
        try (Scanner s = ScannerBuilder.create(accumuloClient).setTableName(accumuloMetadataTable)
                        .setAuthorizations(accumuloClient.securityOperations().getUserAuthorizations(accumuloClient.whoami())).build()) {
            s.setRange(new Range(new Key(tableId + ";" + startRow), true, new Key(tableId + endRow), true));
            s.fetchColumnFamily("file");
            Iterator<Map.Entry<Key,Value>> metarator = s.iterator();
            while (metarator.hasNext()) {
                Map.Entry<Key,Value> next = metarator.next();
                ByteSequence file = next.getKey().getColumnQualifierData();
                Text nextRow = next.getKey().getRow();
                if (currentRow == null || !currentRow.equals(nextRow)) {
                    if (files != null) {
                        String split = currentRow.toString().substring(currentRow.toString().indexOf(';') + 1);
                        metadataFiles.add(new AbstractMap.SimpleEntry<>(split, files));
                    }
                    currentRow = nextRow;
                    files = new ArrayList<>();
                }

                files.add(getFilePath(file.toString(), allowFencedFiles));
            }

            if (files != null && !files.isEmpty()) {
                String fullRow = currentRow.toString();
                int splitIndex = fullRow.indexOf(';');
                if (splitIndex == -1) {
                    splitIndex = fullRow.indexOf('<');
                }
                String split = fullRow;
                if (splitIndex != -1) {
                    split = fullRow.substring(splitIndex + 1);
                }
                metadataFiles.add(new AbstractMap.SimpleEntry<>(split, files));
            }
        } catch (AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException("Failed to scan metadata table " + accumuloMetadataTable + " for table " + tableName, e);
        }

        return metadataFiles;
    }

    /**
     * Gets a file's path from its entry in the metadata table's file column. Accumulo 4 writes the entry as JSON with the path and the rows of the file the
     * tablet uses, its fence, which a merge or deleteRows sets. Earlier versions wrote just the path.
     *
     * @param fileEntry
     *            the file column qualifier
     * @param allowFencedFiles
     *            whether to return the path of a fenced file
     * @return the file's path
     * @throws IllegalStateException
     *             if the file is fenced and allowFencedFiles is false
     */
    static String getFilePath(String fileEntry, boolean allowFencedFiles) {
        if (!fileEntry.startsWith("{")) {
            return fileEntry;
        }
        JsonObject json = JsonParser.parseString(fileEntry).getAsJsonObject();
        String path = json.get("path").getAsString();
        // the start and end rows are empty when the tablet uses the whole file
        if (!allowFencedFiles && (isSet(json.get("startRow")) || isSet(json.get("endRow")))) {
            throw new IllegalStateException(path + " is fenced to part of its rows, so reading it directly would read rows outside its tablet."
                            + " Compacting the tablet rewrites it without the fence.");
        }
        return path;
    }

    private static boolean isSet(JsonElement row) {
        return row != null && !row.isJsonNull() && !row.getAsString().isEmpty();
    }
}
