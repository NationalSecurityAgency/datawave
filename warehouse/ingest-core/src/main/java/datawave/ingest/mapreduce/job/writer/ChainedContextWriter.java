/**
 *
 */
package datawave.ingest.mapreduce.job.writer;

import org.apache.hadoop.conf.Configuration;

/**
 * This is a context writer that will delegate to another context writer after doing whatever it needs to do.
 *
 *
 *
 */
public interface ChainedContextWriter<OK,OV> extends ContextWriter<OK,OV> {
    void configureChainedContextWriter(Configuration conf, Class<? extends ContextWriter<OK,OV>> contextWriterClass);
}
