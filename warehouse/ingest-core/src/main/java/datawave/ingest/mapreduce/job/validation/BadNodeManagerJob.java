package datawave.ingest.mapreduce.job.validation;

import java.io.IOException;
import java.net.InetAddress;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class BadNodeManagerJob extends Configured implements Tool {

    public static class FailOnSumulatedBadHostsMapper extends Mapper<LongWritable,Text,Text,Text> {
        private List<String> badHosts;

        @Override
        protected void setup(Context context) {
            badHosts = List.of(context.getConfiguration().get("bad.host").split(","));
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String hostname = InetAddress.getLocalHost().getHostName();
            if (badHosts.contains(hostname)) {
                Thread.sleep(5000);
                throw new RuntimeException("Failing on bad host: " + hostname);
            } else {
                Thread.sleep(30000);
                context.write(new Text("OK"), new Text("1"));
            }
        }
    }

    public static class FaileOnSimulatedBadHostsReducer extends Reducer<Text,Text,Text,Text> {
        private List<String> badHosts;

        @Override
        protected void setup(Context context) {
            badHosts = List.of(context.getConfiguration().get("bad.host").split(","));
        }

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String hostname = InetAddress.getLocalHost().getHostName();
            System.out.println(hostname);
            if (badHosts.contains(hostname)) {
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

        conf.set("mapreduce.map.memory.mb", "50");
        conf.set("mapreduce.reduce.memory.mb", "50");
        conf.set("mapreduce.map.java.opts", "-Xmx40m");
        conf.set("mapreduce.reduce.java.opts", "-Xmx40m");

        conf.set("bad.host.list", args[0]);

        Job job = Job.getInstance(conf, "BadNodeManagerJob");
        job.setJarByClass(BadNodeManagerJob.class);

        job.setMapperClass(FailOnSumulatedBadHostsMapper.class);
        job.setReducerClass(FaileOnSimulatedBadHostsReducer.class);

        conf.set("datawave.bad.nodemanager.tasks", args[1]);
        job.setNumReduceTasks(Integer.parseInt(args[1]));

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(FixedMapperInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Dummy Output Path
        TextOutputFormat.setOutputPath(job, new org.apache.hadoop.fs.Path("/tmp/output-" + System.currentTimeMillis()));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new BadNodeManagerJob(), args);
        System.exit(res);
    }
}
