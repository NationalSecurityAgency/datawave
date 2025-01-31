package datawave.ingest.mapreduce.job.util;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class FilePathInputFormat extends InputFormat<LongWritable,Text> {

    @Override
    public List<InputSplit> getSplits(JobContext jobContext) throws IOException {
        List<InputSplit> splits = new ArrayList<>();
        Configuration conf = jobContext.getConfiguration();
        Path inputPath = new Path(conf.get("mapreduce.input.filepath")); // Pass directory via config
        FileSystem fs = inputPath.getFileSystem(conf);
        FileStatus[] fileStatuses = fs.listStatus(inputPath);

        for (FileStatus fileStatus : fileStatuses) {
            if (fileStatus.isDirectory()) {
                FileStatus[] filesInDirectory = fs.listStatus(fileStatus.getPath());

                // Check if any file is named "job.complete"
                boolean containsJobComplete = Arrays.stream(filesInDirectory).anyMatch(file -> file.getPath().getName().equals("job.complete"));

                if (containsJobComplete) {
                    splits.add(new FileSplit(fileStatus.getPath(), 0, 0, null)); // Create a fake split for each file
                }
            }
        }

        return splits;
    }

    @Override
    public RecordReader<LongWritable,Text> createRecordReader(InputSplit split, TaskAttemptContext context) {
        return new FilePathRecordReader();
    }

    public static class FilePathRecordReader extends RecordReader<LongWritable,Text> {
        private Path filePath;
        private boolean read = false;

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {
            this.filePath = ((FileSplit) split).getPath();
        }

        @Override
        public boolean nextKeyValue() {
            if (!read) {
                read = true;
                return true;
            }
            return false;
        }

        @Override
        public LongWritable getCurrentKey() {
            return new LongWritable(1);
        }

        @Override
        public Text getCurrentValue() {
            return new Text(filePath.toString());
        }

        @Override
        public float getProgress() {
            return read ? 1.0f : 0.0f;
        }

        @Override
        public void close() {}
    }
}
