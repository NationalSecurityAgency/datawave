package datawave.ingest.mapreduce.job.util;

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.accumulo.core.data.LoadPlan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import datawave.ingest.mapreduce.job.SplitsFile;
import datawave.ingest.mapreduce.job.TableSplitsCache;
import datawave.util.StringUtils;

public class GenerateLoadPlanDriver {

    private static String cacheBaseDir;
    protected final static Logger log = Logger.getLogger("generateLoadPlan");
    private static List<Path> jobDependencies = new ArrayList<>();
    private static String baseDir;

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        if (args.length < 6) {
            System.err.println("Usage: GenerateLoadPlanDriver -cacheBaseDir <job-cache-dir> " + "-cacheJars <jars-to-use-from-cache>"
                            + " -biDir <input-direcotry> -splitsCacheDir <splits-cache-dir> ");
            System.exit(-1);
        }
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-cacheBaseDir")) {
                cacheBaseDir = args[++i];
            } else if (args[i].equals("-cacheJars")) {
                String[] jars = StringUtils.trimAndRemoveEmptyStrings(args[++i].replaceAll("\\s+", "").split(","));
                for (String jarString : jars) {
                    File jar = new File(jarString);
                    Path file = new Path(cacheBaseDir, jar.getName());
                    log.info("Adding " + file + " to job class path via distributed cache.");
                    jobDependencies.add(file);
                }
            } else if (args[i].equals("-biDir")) {
                baseDir = args[++i].startsWith("hdfs://") ? args[i] : "hdfs://" + args[i];
            } else if (args[i].equals("-splitsCacheDir")) {
                conf.set(TableSplitsCache.SPLITS_CACHE_DIR, args[++i]);
            }
        }

        Job job = Job.getInstance(conf, "Generate Load Plan");
        job.setJarByClass(GenerateLoadPlanDriver.class);
        job.setMapperClass(LoadPlanMapper.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.getConfiguration().set("mapreduce.input.filepath", baseDir);
        // job.getConfiguration().setInt("mapreduce.map.memory.mb", 6140);
        // job.getConfiguration().set("mapred.child.java.opts", "-Xmx6140m -Xms6140m");
        job.setInputFormatClass(FilePathInputFormat.class);
        FileOutputFormat.setOutputPath(job, new Path(baseDir + "/output"));

        if (null == conf.get(SplitsFile.SPLIT_WORK_DIR)) {
            conf.set(SplitsFile.SPLIT_WORK_DIR, baseDir + "/splits");
        }
        conf.setInt("splits.num.reduce", 1);
        SplitsFile.setupFile(job, conf);
        for (Path dependency : jobDependencies)
            job.addFileToClassPath(dependency);
        System.out.println("Submitting job...");
        job.waitForCompletion(true);

        System.out.println("All jobs completed successfully!");
    }
}
