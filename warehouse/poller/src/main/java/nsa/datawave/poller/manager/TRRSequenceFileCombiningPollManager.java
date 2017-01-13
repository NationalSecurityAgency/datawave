package nsa.datawave.poller.manager;

import java.io.File;
import java.io.IOException;

import nsa.datawave.poller.manager.io.CountingSequenceFileOutputStream;
import nsa.datawave.poller.manager.io.TranslatingRecordReader;
import nsa.datawave.poller.manager.mapreduce.StandaloneTaskAttemptContext;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Implementation of SequenceFileCombiningPollManager that works with TranslatingRecordReaders
 * 
 */
public class TRRSequenceFileCombiningPollManager extends SequenceFileCombiningPollManager {
    
    /**
     * Creates an instance of a RecordReader
     * 
     * @param <K>
     * @param <V>
     * @param kc
     * @param vc
     * @param recordReaderClass
     * @return
     * @throws Exception
     */
    @SuppressWarnings("unchecked")
    private <K,V> RecordReader<K,V> getRecordReader(K kc, V vc, String recordReaderClass) throws Exception {
        Class<RecordReader<K,V>> reader = (Class<RecordReader<K,V>>) Class.forName(recordReaderClass);
        return reader.newInstance();
    }
    
    @SuppressWarnings("unchecked")
    private <K,V> void setupTRR(K kc, V vc, InputSplit is, TaskAttemptContext ctx, RecordReader<?,?> ingestRecordReader) throws IOException,
                    InterruptedException {
        // Set the parent record reader to the source
        RecordReader<K,V> source = (RecordReader<K,V>) reader;
        TranslatingRecordReader<K,V,?,?> trr = (TranslatingRecordReader<K,V,?,?>) ingestRecordReader;
        trr.setup(job);
        trr.setSource(source);
        trr.setSkipCount(0);
    }
    
    /**
     * This method overrides the replaces the InputFormat's reader with the one specified by ingest.reader
     */
    @Override
    protected boolean processKeyValues(File inputFile, RecordReader<?,?> reader, InputSplit split, StandaloneTaskAttemptContext<?,?,?,?> ctx,
                    CountingSequenceFileOutputStream out) throws Exception {
        
        // super.reader has been setup by the parent class. It is something like LongLineEventRecordReader
        
        // Create the Reader class
        String readerClass = job.getConfiguration().get("ingest.reader");
        RecordReader<?,?> ingestReader = getRecordReader(kClass, vClass, readerClass);
        
        // Initialize the RecordReader for this file.
        if (ingestReader instanceof TranslatingRecordReader<?,?,?,?>) {
            setupTRR(this.kClass, this.vClass, split, ctx, ingestReader);
        }
        
        return super.processKeyValues(inputFile, ingestReader, split, ctx, out);
    }
    
}
