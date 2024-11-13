package datawave.ingest.mapreduce.handler;

import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.google.common.collect.Multimap;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.Type;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.data.config.ingest.IngestHelperInterface;
import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.metadata.RawRecordMetadata;

/**
 * Generic high level interface for processing Events. The EventMapper class uses instances of this interface to process Event objects that are read from the
 * RecordReader.
 *
 *
 *
 * @param <KEYIN>
 *            type of data type handler
 */
public interface DataTypeHandler<KEYIN> {

    Value NULL_VALUE = new Value(new byte[0]);

    void setup(TaskAttemptContext context);

    /**
     * Return the list of tables that are used by this handler. Note that the handler should NOT have to be "setup" to call this method.
     *
     * @param conf
     *            the configuration
     * @return list of table names
     */
    String[] getTableNames(Configuration conf);

    /**
     * Return the list of table priorities that are used by this handler. Note that the handler should NOT have to be "setup" to call this method.
     *
     * @param conf
     *            the configuration
     * @return a list of table priorities
     */
    int[] getTableLoaderPriorities(Configuration conf);

    /**
     * This method is called by the EventMapper to process the current Event for Bulk ingest.
     *
     * @param key
     *            a key
     * @param event
     *            a event container
     * @param fields
     *            fields of event
     * @param reporter
     *            a status reporter
     * @return Map of Key,Value pairs or null if error.
     */
    Multimap<BulkIngestKey,Value> processBulk(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields, StatusReporter reporter);

    /**
     * DataType specific helper object
     *
     * @param datatype
     *            - datatype of the event that is being processed. This will be used for DataTypeHandlers that handle more than one type of data (i.e. Edge),
     *            otherwise may be ignored.
     * @return helper object used in the subclass
     */
    IngestHelperInterface getHelper(Type datatype);

    void close(TaskAttemptContext context);

    /**
     * Get the metadata producer if any
     *
     * @return The EventMetadata object
     */
    RawRecordMetadata getMetadata();

}
