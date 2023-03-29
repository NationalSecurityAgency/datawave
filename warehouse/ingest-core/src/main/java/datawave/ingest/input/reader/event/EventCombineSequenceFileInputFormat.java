package datawave.ingest.input.reader.event;

import datawave.ingest.data.RawRecordContainer;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReaderWrapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.CombineSequenceFileInputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Specialization of SequenceFileInputFormat that expects an {@link RawRecordContainer} object as the value.
 *
 *
 *
 * @param <K>
 *            key type
 */
public class EventCombineSequenceFileInputFormat<K> extends CombineSequenceFileInputFormat<K,RawRecordContainer> {
    
    @Override
    public RecordReader<K,RawRecordContainer> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new CombineFileRecordReader((CombineFileSplit) split, context, EventSequenceFileRecordReaderWrapper.class);
    }
    
    private static class EventSequenceFileRecordReaderWrapper<K> extends CombineFileRecordReaderWrapper<K,RawRecordContainer> {
        // this constructor signature is required by CombineFileRecordReader
        public EventSequenceFileRecordReaderWrapper(CombineFileSplit split, TaskAttemptContext context, Integer idx) throws IOException, InterruptedException {
            super(new EventSequenceFileInputFormat<K>(), split, context, idx);
        }
    }
}
