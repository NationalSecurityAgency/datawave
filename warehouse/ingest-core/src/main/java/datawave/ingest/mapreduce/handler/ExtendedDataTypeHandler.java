package datawave.ingest.mapreduce.handler;

import java.io.IOException;

import datawave.ingest.data.RawRecordContainer;
import datawave.ingest.data.config.NormalizedContentInterface;
import datawave.ingest.mapreduce.job.writer.ContextWriter;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

import org.apache.accumulo.core.data.Value;

import com.google.common.collect.Multimap;

/**
 * Generic high level interface for processing Events. The EventMapper class uses instances of this interface to process Event objects that are read from the
 * RecordReader.
 * 
 * 
 * 
 * @param <KEYIN>
 *            type for extendeddatatypehandler
 */
public interface ExtendedDataTypeHandler<KEYIN,KEYOUT,VALUEOUT> extends DataTypeHandler<KEYIN> {
    
    Value NULL_VALUE = new Value(new byte[0]);
    String FULL_CONTENT_LOCALITY_NAME = "fullcontent";
    String FULL_CONTENT_COLUMN_FAMILY = "d";
    /* TODO Make a clearer definition of full content indexers */
    String TERM_FREQUENCY_LOCALITY_NAME = "termfrequency";
    Text TERM_FREQUENCY_COLUMN_FAMILY = new Text("tf");
    
    long process(KEYIN key, RawRecordContainer event, Multimap<String,NormalizedContentInterface> fields,
                    TaskInputOutputContext<KEYIN,? extends RawRecordContainer,KEYOUT,VALUEOUT> context, ContextWriter<KEYOUT,VALUEOUT> contextWriter)
                    throws IOException, InterruptedException;
    
}
