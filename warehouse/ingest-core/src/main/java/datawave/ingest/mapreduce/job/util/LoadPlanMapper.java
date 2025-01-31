package datawave.ingest.mapreduce.job.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;

import org.apache.accumulo.core.data.LoadPlan;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import datawave.ingest.mapreduce.job.SplitsFile;

public class LoadPlanMapper extends Mapper<LongWritable,Text,Text,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Path workingPath = new Path(value.toString());
        FileSystem fs = workingPath.getFileSystem(conf);
        Map<String,List<Text>> splitsMap = SplitsFile.getSplits(conf);

        for (Path subDir : org.apache.hadoop.fs.FileUtil.stat2Paths(fs.listStatus(new Path(workingPath, "mapFiles")))) {
            if (fs.getFileStatus(subDir).isDirectory()) {
                String subDirectoryName = subDir.getName();

                Path outputForTable = new Path(subDir, subDirectoryName + "-loadplan.json");
                if (!fs.exists(outputForTable)) {
                    // Prepare job configuration for the subdirectory
                    LoadPlan.SplitResolver splitResolver = SplitsFile
                                    .createSplitResolver(splitsMap.getOrDefault(subDirectoryName, splitsMap.getOrDefault(subDirectoryName, new ArrayList<>())));

                    LoadPlan.Builder builder = LoadPlan.builder();

                    for (FileStatus fileStatus : fs.listStatus(subDir)) {
                        if (!fileStatus.isDirectory() && fileStatus.getPath().getName().endsWith(".rf")) {
                            Path filePath = fileStatus.getPath();
                            builder.addPlan(LoadPlan.compute(filePath.toUri(), splitResolver));
                        }
                    }

                    LoadPlan lp = builder.build();
                    if (!lp.getDestinations().isEmpty()) {
                        try (FSDataOutputStream out = fs.create(outputForTable, true)) {
                            out.writeBytes(lp.toJson());
                        }
                    }
                }
            }
        }
    }
}
