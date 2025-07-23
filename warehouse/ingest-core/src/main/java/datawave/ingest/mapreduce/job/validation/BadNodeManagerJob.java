package datawave.ingest.mapreduce.job.validation;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.io.IOException;
import java.net.InetAddress;


public class BadNodeManagerJob extends Configured implements Tool {

    public static class DummyMapper extends Mapper<LongWritable, Text, Text, Text> {
        private String badHost;

        @Override
        protected void setup(Context context) {
            badHost = context.getConfiguration().get("bad.host");
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String hostname = InetAddress.getLocalHost().getHostName();
            if (hostname.equals(badHost)) {
                Thread.sleep(5000);
                throw new RuntimeException("Failing on bad host: " + hostname);
            } else {
                Thread.sleep(30000);
                context.write(new Text("OK"), new Text("1"));
            }
        }
    }

    public static class DummyReducer extends Reducer<Text, Text, Text, Text> {
        private String badHost;

        @Override
        protected void setup(Context context) {
            badHost = context.getConfiguration().get("bad.host");
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String hostname = InetAddress.getLocalHost().getHostName();
            System.out.println(hostname);
            if (hostname.equals(badHost)) {
                Thread.sleep(5000);
                throw new RuntimeException("Reducer failing on bad host: " + hostname);
            } else {
                Thread.sleep(30000);
                context.write(key, new Text("done"));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        conf.set("mapreduce.map.memory.mb", "500");
        conf.set("mapreduce.reduce.memory.mb", "500");
        conf.set("mapreduce.map.java.opts", "-Xmx400m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx400m");

        conf.set("bad.host", args[0]);

        Job job = Job.getInstance(conf, "HostnameFailJob");
        job.setJarByClass(BadNodeManagerJob.class);

        job.setMapperClass(DummyMapper.class);
        job.setReducerClass(DummyReducer.class);

        job.setNumReduceTasks(50);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        job.setInputFormatClass(FixedMapperInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Use dummy input
        TextOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("/tmp/output-" + System.currentTimeMillis()));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BadNodeManagerJob(), args);
        System.exit(res);
    }
}