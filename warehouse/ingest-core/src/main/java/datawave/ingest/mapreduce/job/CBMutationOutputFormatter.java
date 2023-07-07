package datawave.ingest.mapreduce.job;

import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.hadoop.mapreduce.AccumuloOutputFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;

public class CBMutationOutputFormatter extends AccumuloOutputFormat {

    private static final Logger log = Logger.getLogger(CBMutationOutputFormatter.class);

    @Override
    public RecordWriter<Text,Mutation> getRecordWriter(TaskAttemptContext attempt) throws IOException {
        return new CBRecordWriter(super.getRecordWriter(attempt), attempt);
    }

    public static class CBRecordWriter extends RecordWriter<Text,Mutation> {
        private RecordWriter<Text,Mutation> delegate;
        private String eventTable = null;

        public CBRecordWriter(RecordWriter<Text,Mutation> writer, TaskAttemptContext context) throws IOException {
            this.delegate = writer;
            eventTable = context.getConfiguration().get(ShardedDataTypeHandler.SHARD_TNAME, "");
            log.info("Event Table Name property for " + ShardedDataTypeHandler.SHARD_TNAME + " is " + eventTable);
        }

        @Override
        public void close(TaskAttemptContext context) throws IOException, InterruptedException {
            delegate.close(context);
        }

        @Override
        public void write(Text key, Mutation value) throws IOException, InterruptedException {
            delegate.write(key, value);
        }
    }
}
