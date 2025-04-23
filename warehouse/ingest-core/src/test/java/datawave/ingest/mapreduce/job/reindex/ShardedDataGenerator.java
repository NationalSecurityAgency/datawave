package datawave.ingest.mapreduce.job.reindex;

import static datawave.ingest.data.config.CSVHelper.DATA_HEADER;
import static datawave.ingest.data.config.CSVHelper.DATA_SEP;
import static datawave.ingest.data.config.DataTypeHelper.Properties.DATA_NAME;
import static datawave.ingest.data.config.ingest.BaseIngestHelper.INDEX_FIELDS;
import static datawave.ingest.data.config.ingest.BaseIngestHelper.INDEX_ONLY_FIELDS;
import static datawave.ingest.data.config.ingest.BaseIngestHelper.REVERSE_INDEX_FIELDS;
import static datawave.ingest.data.config.ingest.ContentBaseIngestHelper.TOKEN_INDEX_ALLOWLIST;
import static datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler.METADATA_TABLE_NAME;
import static datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler.SHARD_GIDX_TNAME;
import static datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler.SHARD_GRIDX_TNAME;
import static datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler.SHARD_TNAME;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.accumulo.core.client.rfile.RFile;
import org.apache.accumulo.core.client.rfile.RFileWriter;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.apache.hadoop.shaded.com.google.common.io.Files;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.StringUtils;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;

import datawave.ingest.config.RawRecordContainerImpl;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.TypeRegistry;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.CSVIngestHelper;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.StandaloneStatusReporter;
import datawave.ingest.mapreduce.handler.DataTypeHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;

public class ShardedDataGenerator {
    public static void setup(Configuration config, String dataType, int shards, String shardTable, String shardIndexTable, String shardReverseIndexTable,
                    String datawaveMetadataTable) {
        String header = "FIELDA,FIELDB,FIELDC,FIELDD,FIELDE,FIELDF,FIELDG,FIELDH,FIELDI";
        List<String> indexedFields = Arrays.asList("FIELDA", "FIELDB", "FIELDC", "FIELDE", "FIELDF", "FIELDG");
        List<String> reverseIndexedFields = Arrays.asList("FIELDB", "FIELDD");
        List<String> indexOnlyFields = Arrays.asList("FIELDE");
        List<String> tokenizedFields = Arrays.asList("FIELDE", "FIELDF", "FIELDG");

        setup(config, dataType, shards, shardTable, shardIndexTable, shardReverseIndexTable, datawaveMetadataTable, indexedFields, reverseIndexedFields,
                        tokenizedFields, indexOnlyFields, header);
    }

    public static void setup(Configuration config, String dataType, int shards, String shardTable, String shardIndexTable, String shardReverseIndexTable,
                    String datawaveMetadataTable, List<String> indexFields, List<String> reverseIndexedFields, List<String> tokenizedFields,
                    List<String> indexOnlyFields, String header) {
        config.addResource(ClassLoader.getSystemResource("config/all-config.xml"));
        config.addResource(ClassLoader.getSystemResource("config/error-ingest-config.xml"));

        config.setInt("num.shards", shards);
        config.set(SHARD_TNAME, shardTable);
        config.set(SHARD_GIDX_TNAME, shardIndexTable);
        config.set(SHARD_GRIDX_TNAME, shardReverseIndexTable);
        config.set(METADATA_TABLE_NAME, datawaveMetadataTable);

        // simple required config for a type with some indexed fields
        config.set(DATA_NAME, dataType);
        config.set(dataType + DATA_HEADER, header);
        config.set(dataType + DATA_SEP, ",");
        config.set(dataType + INDEX_FIELDS, StringUtils.join(',', indexFields));
        config.set(dataType + REVERSE_INDEX_FIELDS, StringUtils.join(',', reverseIndexedFields));
        config.set(dataType + INDEX_ONLY_FIELDS, StringUtils.join(',', indexOnlyFields));
        config.set(dataType + TOKEN_INDEX_ALLOWLIST, StringUtils.join(',', tokenizedFields));

        Type t = new Type(dataType, CSVIngestHelper.class, null, null, 0, null);
        // this needs to be called each test to clear any static config that may be cached
        t.clearIngestHelper();

        TypeRegistry.reset();
        TypeRegistry registry = TypeRegistry.getInstance(config);
        registry.put(t.typeName(), t);
    }

    public static DataTypeHandler getDataTypeHandler(Configuration config, TaskAttemptContext context, String handlerClass) throws ClassNotFoundException {
        DataTypeHandler handler = (DataTypeHandler) ReflectionUtils.newInstance(Class.forName(handlerClass), config);
        handler.setup(context);

        return handler;
    }

    public static Multimap<BulkIngestKey,Value> process(Configuration config, DataTypeHandler handler, String dataType, RawRecordContainer event,
                    StatusReporter reporter) {
        Type t = TypeRegistry.getInstance(config).get(dataType);
        IngestHelperInterface helper = handler.getHelper(t);
        Multimap<String,NormalizedContentInterface> eventFields = helper.getEventFields(event);
        return handler.processBulk(null, event, eventFields, reporter);
    }

    public static RawRecordContainer generateEvent(Configuration config, String dataType, Date date, byte[] rawData, ColumnVisibility visibility) {
        RawRecordContainer event = new RawRecordContainerImpl();
        event.setRawData(rawData);
        event.setDate(date.getTime());
        event.generateId(null);
        Type t = TypeRegistry.getInstance(config).get(dataType);
        event.setDataType(t);

        event.setVisibility(visibility);

        return event;
    }

    public static String generateRawData(int fieldCount, List<String> options) {
        List<String> values = new ArrayList<>();
        Random r = new Random();
        for (int i = 0; i < fieldCount; i++) {
            String value = options.get(r.nextInt(options.size()));
            values.add(value);
        }

        return StringUtils.join(',', values);
    }

    public static void writeData(String outputDir, String filename, Multimap<BulkIngestKey,Value> data) throws IOException, InterruptedException {
        Multimap<BulkIngestKey,Value> treeMap = TreeMultimap.create(data);

        Path dir = new Path(outputDir);
        Map<String,RFileWriter> writerMap = new HashMap<>();
        for (BulkIngestKey bik : treeMap.keySet()) {
            RFileWriter writer = writerMap.get(bik.getTableName().toString());
            if (writer == null) {
                Path table = new Path(dir, new Path(bik.getTableName().toString()));
                Path rfile = new Path(table, new Path(filename));
                writer = RFile.newWriter().to(rfile.toString()).build();
                writerMap.put(bik.getTableName().toString(), writer);
            }
            for (Value v : treeMap.get(bik)) {
                writer.append(bik.getKey(), v);
            }
        }

        for (RFileWriter writer : writerMap.values()) {
            writer.close();
        }
    }

    public static File createIngestFiles(List<String> dataOptions, int numEvents) throws ClassNotFoundException, IOException, InterruptedException {
        File f = Files.createTempDir();
        Configuration config = new Configuration();
        ShardedDataGenerator.setup(config, "samplecsv", 8, "shard", "shardIndex", "shardReverseIndex", "DatawaveMetadata");
        DataTypeHandler handler = ShardedDataGenerator.getDataTypeHandler(config, new TaskAttemptContextImpl(config, new TaskAttemptID()),
                        SimpleContentIndexingColumnBasedHandler.class.getCanonicalName());
        Multimap<BulkIngestKey,Value> allGenerated = HashMultimap.create();
        for (int i = 0; i < numEvents; i++) {
            RawRecordContainer container = ShardedDataGenerator.generateEvent(config, "samplecsv", new Date(),
                            ShardedDataGenerator.generateRawData(9, dataOptions).getBytes(), new ColumnVisibility());
            Multimap<BulkIngestKey,Value> generated = ShardedDataGenerator.process(config, handler, "samplecsv", container, new StandaloneStatusReporter());
            allGenerated.putAll(generated);
        }
        ShardedDataGenerator.writeData(f.getAbsolutePath(), "magic.rf", allGenerated);

        return f;
    }

    public static File createIngestFiles(List<String> dataOptions) throws ClassNotFoundException, IOException, InterruptedException {
        return createIngestFiles(dataOptions, 1);
    }
}
