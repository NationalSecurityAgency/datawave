package datawave.ingest.mapreduce.job.reload;

import java.io.IOException;
import java.util.Map;

import org.apache.accumulo.core.client.Accumulo;
import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import datawave.ingest.mapreduce.job.BulkIngestKey;
import datawave.ingest.mapreduce.job.MultiRFileOutputFormatter;

public class CreateIDTableJob {
    public static class GenerateIDTable extends Mapper<LongWritable,Text,BulkIngestKey,Text> {

        protected final Logger log = LoggerFactory.getLogger(getClass());

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            log.info("Processing Row: \"{}", value.toString());

            Configuration conf = context.getConfiguration();
            Text tableText = new Text(conf.getStrings("create.id.destination.table.name")[0]);
            String tableName = conf.getStrings("create.id.source.table.name")[0];

            String fieldName = conf.getStrings("create.id.table.field.name")[0];
            Text EMPTY_TEXT = new Text();
            byte[] fieldNameBytes = new Text(fieldName).getBytes();
            // Retrieve Accumulo properties from job configuration
            String instanceName = conf.get("accumulo.instanceName");
            String zooKeepers = conf.get("accumulo.zooKeepers");
            String user = conf.get("accumulo.user");
            String passToken = conf.get("accumulo.pass");
            Range range = new Range(value.toString());

            AccumuloClient accumuloClient = Accumulo.newClient().to(instanceName, zooKeepers).as(user, passToken).build();
            Scanner scanner = null;
            try {
                scanner = accumuloClient.createScanner(tableName);
                scanner.setRange(range);
                scanner.fetchColumnFamily("fi\0" + fieldName);
                for (Map.Entry<Key,Value> e : scanner) {
                    Text ident = new Text(e.getKey().getColumnFamily().toString().split("\0")[0]);
                    Text shardId = new Text(value.toString());
                    Key outputKey = new Key(ident.getBytes(), shardId.getBytes(), fieldNameBytes, EMPTY_TEXT.getBytes(), 0, false);
                    context.write(new BulkIngestKey(tableText, outputKey), EMPTY_TEXT);
                }
            } catch (TableNotFoundException | AccumuloException | AccumuloSecurityException e) {
                throw new RuntimeException(e);
            }

        }

        public static void main(String[] args) throws Exception {

            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Create ID Table Job");
            conf.set("create.id.table.field.name", "id");

            // Set Accumulo properties in the job configuration
            conf.set("accumulo.instanceName", "dev"); // Replace with actual instance name
            conf.set("accumulo.zooKeepers", "zookeeper:2181"); // Replace with actual Zookeeper connection string
            conf.set("accumulo.user", "datawave"); // Replace with actual username
            conf.set("accumulo.pass", System.getenv("PASSWORD")); // Pass or token, ensure it’s safely stored

            job.setJarByClass(CreateIDTableJob.class);

            job.setMapperClass(GenerateIDTable.class);
            // ??
            job.setNumReduceTasks(1999);

            job.setInputFormatClass(TextInputFormat.class);

            job.setOutputFormatClass(MultiRFileOutputFormatter.class);

            TextInputFormat.addInputPath(job, new Path(args[0])); // Input is text strings

            System.exit(job.waitForCompletion(true) ? 0 : 1);
        }
    }
}
