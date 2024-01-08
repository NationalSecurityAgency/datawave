package datawave.ingest.mapreduce.handler.ssdeep;

import com.google.common.collect.Multimap;
import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.handler.ExtendedDataTypeHandler;
import datawave.ingest.mapreduce.handler.facet.FacetHandler;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.writer.ContextWriter;
import datawave.ingest.metadata.RawRecordMetadata;
import datawave.marking.MarkingFunctions;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SSDeepIndexHandler<KEYIN,KEYOUT,VALUEOUT> implements ExtendedDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> {
    private static final Logger log = Logger.getLogger(SSDeepIndexHandler.class);


    /* Global configuration properties */

    public static final String SSDEEP_INDEX_TABLE_NAME = "ssdeepIndex.table.name";
    public static final String SSDEEP_INDEX_TABLE_LOADER_PRIORITY = "ssdeepIndex.table.loader.priority";

    protected Text ssdeepIndexTableName;

    protected MarkingFunctions markingFunctions;

    protected TaskAttemptContext taskAttemptContext;

    @Override
    public void setup(TaskAttemptContext context) {

    }
    @Override
    public String[] getTableNames(Configuration conf) {
        return getNonNullTableNames(conf, SSDEEP_INDEX_TABLE_NAME);
    }

    @Override
    public int[] getTableLoaderPriorities(Configuration conf) {
        // @formatter:off
        return getNonNullTableLoaderPriorities(
                conf, 40,
                SSDEEP_INDEX_TABLE_NAME, SSDEEP_INDEX_TABLE_LOADER_PRIORITY);
        // @formatter:on
    }

    @Override
    public Multimap<BulkIngestKey, Value> processBulk(KEYIN key, RawRecordContainer event, Multimap<String, NormalizedContentInterface> fields,
                                                      StatusReporter reporter) {
        throw new UnsupportedOperationException("processBulk is not supported, please use process");
    }

    @Override
    public IngestHelperInterface getHelper(Type datatype) {
        return datatype.getIngestHelper(this.taskAttemptContext.getConfiguration());
    }

    @Override
    public void close(TaskAttemptContext context) {
        /* no-op */
    }

    @Override
    public RawRecordMetadata getMetadata() {
        return null;
    }

    protected byte[] flatten(ColumnVisibility vis) {
        return markingFunctions == null ? vis.flatten() : markingFunctions.flatten(vis);
    }

    @Override
    public long process(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                        TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
            throws IOException, InterruptedException {

        //TODO generate ssdeep index entries
        return 0L;
    }

    /**
     * Return an array of table loader priorities extracted from the specified configuration based on the property names supplied, where those properties are
     * present in the configuration.
     *
     * @param conf
     *            the configuration to extract the priority values from
     * @param defaultPriority
     *            the defailt priority to use if none is specified.
     * @param properties
     *            the properties to read from the configuration, must be an even number organized based on table name property name, table priority property
     *            name.
     * @return an array of the table priorities loaded from the configuration the length of this will be based on the number of the specified table name
     *         properties present in the configuration.
     */
    protected int[] getNonNullTableLoaderPriorities(Configuration conf, int defaultPriority, String... properties) {
        if (properties.length % 2 != 0) {
            throw new IllegalArgumentException("Received an odd number of properties, expected an even number, "
                    + "each table name property should be followed by the table priority property");
        }

        int[] priorities = new int[properties.length / 2];
        int index = 0;

        for (int i = 0; i < properties.length; i += 2) {
            final String tableNameProp = properties[i];
            final String tablePriorityProp = properties[i + 1];
            final String tableName = conf.get(tableNameProp, null);
            if (null != tableName)
                priorities[index++] = conf.getInt(tablePriorityProp, defaultPriority);
        }

        if (index != priorities.length) {
            return Arrays.copyOf(priorities, index);
        } else {
            return priorities;
        }
    }

    /**
     * Return an array of table names extracted from the specified configuration based on the property names supplied, where those properties are present in the
     * configuration.
     *
     * @param conf
     *            the configuration to extract the table names from.
     * @param properties
     *            the properties to read from the configuration in order to obtain table names
     * @return an array of the table priorities loaded from the configuration. The length of this will be dependent on the number of specified properties
     *         present in the configuration.
     */
    protected String[] getNonNullTableNames(Configuration conf, String... properties) {
        final List<String> tableNames = new ArrayList<>();

        for (final String p : properties) {
            final String tableName = conf.get(p, null);
            if (null != tableName)
                tableNames.add(tableName);
        }

        return tableNames.toArray(new String[0]);
    }
}
