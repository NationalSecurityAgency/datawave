package datawave.ingest.mapreduce.job;

import java.io.IOException;
import java.util.Properties;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.hadoop.mapreduce.AccumuloOutputFormat;
import org.apache.accumulo.hadoop.mapreduce.OutputFormatBuilder;
import org.apache.accumulo.hadoopImpl.mapreduce.OutputFormatBuilderImpl;
import org.apache.accumulo.hadoopImpl.mapreduce.lib.OutputConfigurator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import datawave.ingest.data.config.ingest.AccumuloHelper;
import datawave.ingest.mapreduce.handler.shard.ShardedDataTypeHandler;

public class CBMutationOutputFormatter extends AccumuloOutputFormat {
    private static final Logger log = Logger.getLogger(CBMutationOutputFormatter.class);

    @Override
    public RecordWriter<Text,Mutation> getRecordWriter(TaskAttemptContext attempt) throws IOException {
        return new CBRecordWriter(super.getRecordWriter(attempt), attempt);
    }

    public static Properties getClientProperties(Configuration conf) {
        // Get any AccumuloClient property customizations (e.g., batch writer config overrides, etc)
        Properties clientProps = OutputConfigurator.getClientProperties(AccumuloOutputFormat.class, conf);
        //@formatter:off
        // Convert DW's connection conf keys into Accumulo-compatible keys as required
        clientProps.putAll(Accumulo.newClientProperties()
                .to(AccumuloHelper.getInstanceName(conf), AccumuloHelper.getZooKeepers(conf))
                .as(AccumuloHelper.getUsername(conf), new PasswordToken(new String(AccumuloHelper.getPassword(conf))))
                .build());
        //@formatter:on
        return clientProps;
    }

    public static OutputFormatBuilder.OutputOptions<Job> configure(Configuration conf) {
        return AccumuloOutputFormat.configure().clientProperties(getClientProperties(conf));
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
