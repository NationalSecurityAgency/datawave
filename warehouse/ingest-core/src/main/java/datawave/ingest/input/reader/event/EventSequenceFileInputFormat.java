package datawave.ingest.input.reader.event;

import java.io.IOException;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;

import datawave.ingest.data.RawRecordContainer;

/**
 * Specialization of SequenceFileInputFormat that expects an {@link RawRecordContainer} object as the value.
 *
 *
 *
 * @param <K>
 *            key type
 */
public class EventSequenceFileInputFormat<K> extends SequenceFileInputFormat<K,RawRecordContainer> {

    @Override
    public RecordReader<K,RawRecordContainer> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new EventSequenceFileRecordReader<>();
    }

}
