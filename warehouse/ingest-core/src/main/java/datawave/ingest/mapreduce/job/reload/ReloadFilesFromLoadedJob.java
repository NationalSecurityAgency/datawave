package datawave.ingest.mapreduce.job.reload;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReloadFilesFromLoadedJob {
    public static class ReloadFilesFromLoadedMapper extends Mapper<LongWritable,Text,Text,Text> {

        protected final Logger log = LoggerFactory.getLogger(getClass());


        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            log.info("Processing file: \"{}", value.toString());

            String[] parts = value.toString().split("/");
            String datatype = parts[3];
            String year = parts[4];
            String month = parts[5];
            String day = parts[6];
            String hour = parts[7];
            String filename = parts[8];

            String dirPath = "/data/loaded/archive/" + datatype + "/" + year + "/" + month + "/" + day + "/";
            Path loadedPath = new Path("hdfs:///data/loaded/" + datatype + "/" + year + "/" + month + "/" + day + "/");
            Path harFilePath = null;
            String harFileDir = "har://" + dirPath;
            String hdfsArchiveDir = "hdfs://" + dirPath;
            // outputDir does not include hour as we will use the same relative path below.
            String outputDir = "hdfs:///data/" + datatype + "/" + year + "/" + month + "/" + day + "/";

            boolean har = false;

            String hourAndFileName = hour + "/" + filename;

            FileSystem hdfs = FileSystem.get(context.getConfiguration());

            boolean errorOnNotFound = context.getConfiguration().getBoolean("reload.files.error.on.not.found", true);

            Path archiveDirPath = new Path(hdfsArchiveDir);
            if (hdfs.exists(archiveDirPath)) {
                FileStatus[] filesInDir = hdfs.listStatus(archiveDirPath); // List all files in the directory

                for (FileStatus fileStatus : filesInDir) {
                    if (fileStatus.getPath().getName().endsWith(".har")) {
                        harFilePath = new Path(harFileDir, fileStatus.getPath().getName());
                        har = true;// Found the .har file
                        break;
                    } else {
                        loadedPath = fileStatus.getPath();
                    }
                }
            }
            if (har) {
                Path fileInHar = new Path(harFilePath, hourAndFileName);
                Path outputPath = new Path(outputDir, hourAndFileName);

                FileSystem harFs = FileSystem.get(fileInHar.toUri(), context.getConfiguration());

                try (InputStream in = harFs.open(fileInHar); OutputStream out = hdfs.create(outputPath, true)) {

                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = in.read(buffer)) > 0) {
                        out.write(buffer, 0, bytesRead);
                    }
                    log.info("Extracted: {} to {}", fileInHar, outputPath);
                } catch (IOException e) {
                    log.error("Failed to extract: {} - {}", fileInHar, e.getMessage());
                }
            } else {

                hdfs.mkdirs(new Path(outputDir, hour));

                String outputPath = outputDir + hourAndFileName;
                try {
                    FileUtil.rename(hdfs, new Path(loadedPath, hourAndFileName), new Path(outputPath));
                } catch (Exception e) {
                    log.error("Failed to rename: {}/{} to {}", loadedPath, hourAndFileName, outputPath);
                    if (errorOnNotFound) {
                        throw new RuntimeException(loadedPath + "/" + hourAndFileName + " not found in either " + "archive directory or raw loaded directory ",
                                        e);
                    }
                }
            }

        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: datawave.ingest.mapreduce.job.reload.ReloadFilesFromLoadedJob <input path> <lines per map> <error on file not found>");
            System.err.println("NOTE: FLAG MAKERS MUST BE STOPPED WHILE RUNNING");
            System.err.println(
                            "File at <input path> should consist paths to loaded files in the format /data/flagged|loaded/<datatype>/<year>/<month>/<day>/<hour>/<filename>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.set("reload.files.error.on.not.found", args[2]);
        Job job = Job.getInstance(conf, "Reprocess Loaded Files Job");
        job.setJarByClass(ReloadFilesFromLoadedJob.class);

        job.setMapperClass(ReloadFilesFromLoadedMapper.class);
        job.setNumReduceTasks(0);

        job.setInputFormatClass(NLineInputFormat.class);
        NLineInputFormat.setNumLinesPerSplit(job, Integer.parseInt(args[1]));

        job.setOutputFormatClass(NullOutputFormat.class);

        NLineInputFormat.addInputPath(job, new Path(args[0]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
