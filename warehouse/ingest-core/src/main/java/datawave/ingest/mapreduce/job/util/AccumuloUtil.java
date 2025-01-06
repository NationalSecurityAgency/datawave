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
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import datawave.ingest.data.config.ingest.AccumuloHelper;

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
        try (Scanner s = accumuloClient.createScanner(accumuloMetadataTable)) {
            s.setRange(new Range(new Key(tableId + ";" + startRow), true, new Key(tableId + endRow), true));
            s.fetchColumnFamily("file");
            Iterator<Map.Entry<Key,Value>> metarator = s.iterator();
            while (metarator.hasNext()) {
                Map.Entry<Key,Value> next = metarator.next();
                ByteSequence file = next.getKey().getColumnQualifierData();
                Text nextRow = next.getKey().getRow();
                if (currentRow == null || !currentRow.equals(nextRow)) {
                    if (files != null) {
                        String split = currentRow.toString().substring(currentRow.toString().indexOf(";") + 1);
                        metadataFiles.add(new AbstractMap.SimpleEntry<>(split, files));
                    }
                    currentRow = nextRow;
                    files = new ArrayList<>();
                }

                files.add(file.toString());
            }

            if (files != null && !files.isEmpty()) {
                String fullRow = currentRow.toString();
                int splitIndex = fullRow.indexOf(";");
                if (splitIndex == -1) {
                    splitIndex = fullRow.indexOf("<");
                }
                String split = fullRow;
                if (splitIndex != -1) {
                    split = fullRow.substring(splitIndex + 1);
                }
                metadataFiles.add(new AbstractMap.SimpleEntry<>(split, files));
            }
        } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
            throw new RuntimeException("Failed to scan metadata table " + accumuloMetadataTable + " for table " + tableName, e);
        }

        return metadataFiles;
    }
}
