package datawave.ingest.input.reader.raw;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import datawave.ingest.data.config.DataTypeHelperImpl;

public class RawInputFormat<K,V> extends FileInputFormat<K,V> {

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException {
        return super.getSplits(context);
    }

    @Override
    public RecordReader<K,V> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        DataTypeHelperImpl d = new DataTypeHelperImpl();
        d.setup(context.getConfiguration());
        @SuppressWarnings("unchecked")
        RecordReader<K,V> reader = (RecordReader<K,V>) d.getType().newRecordReader();
        if (reader == null) {
            throw new IllegalArgumentException(d.getType().typeName() + " not handled in RawInputFormat");
        }

        return reader;
    }

}
