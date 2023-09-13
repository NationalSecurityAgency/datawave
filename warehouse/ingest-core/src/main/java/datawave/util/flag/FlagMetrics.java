package datawave.util.flag;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Counters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import datawave.ingest.mapreduce.StandaloneStatusReporter;
import datawave.ingest.mapreduce.StandaloneTaskAttemptContext;

/**
 * Collects and reports FlagMaker metrics.
 */
public class FlagMetrics {

    private static final Logger log = LoggerFactory.getLogger(FlagMetrics.class);

    private static final CompressionCodec cc = new GzipCodec();
    private static final SequenceFile.CompressionType ct = SequenceFile.CompressionType.BLOCK;

    private final StandaloneTaskAttemptContext<?,?,?,?> ctx;

    private final FileSystem fs;

    private final boolean enabled;

    /**
     *
     * @param hadoopFS
     *            HDFS file system object
     * @param enableMetrics
     *            enable write of metrics
     */
    FlagMetrics(FileSystem hadoopFS, boolean enableMetrics) {
        this.fs = hadoopFS;
        this.enabled = enableMetrics;
        this.ctx = new StandaloneTaskAttemptContext<>(new Configuration(), new StandaloneStatusReporter());
        ctx.putIfAbsent(datawave.metrics.util.flag.InputFile.FLAGMAKER_START_TIME, System.currentTimeMillis());
    }

    protected void updateCounter(String groupName, String counterName, long val) {
        if (enabled) {
            ctx.getCounter(groupName, counterName).setValue(val);
        }
    }

    protected void writeMetrics(final String metricsDirectory, final String baseName) throws IOException {
        if (!this.enabled) {
            return;
        }

        ctx.getCounter(datawave.metrics.util.flag.InputFile.FLAGMAKER_END_TIME).setValue(System.currentTimeMillis());
        final StandaloneStatusReporter reporter = ctx.getReporter();
        if (reporter == null)
            return;

        ctx.getCounter(datawave.metrics.util.flag.InputFile.FLAGMAKER_END_TIME).setValue(System.currentTimeMillis());
        final Counters counters = reporter.getCounters();
        if (counters == null || counters.countCounters() <= 0)
            return;

        final String baseFileName = metricsDirectory + File.separator + baseName + ".metrics";

        String fileName = baseFileName;
        Path finishedMetricsFile = new Path(fileName);
        Path src = new Path(fileName + ".working");
        if (!fs.exists(finishedMetricsFile.getParent())) {
            if (!fs.mkdirs(finishedMetricsFile.getParent())) {
                log.warn("unable to create directory ({}) metrics write terminated", finishedMetricsFile.getParent());
                return;
            }
        }

        if (!fs.exists(src.getParent())) {
            if (!fs.mkdirs(src.getParent())) {
                log.warn("unable to create directory ({}) metrics write terminated", src.getParent());
                return;
            }
        }

        int count = 0;

        while (true) {
            while (fs.exists(finishedMetricsFile) || !fs.createNewFile(src)) {
                count++;

                fileName = baseFileName + '.' + count;
                finishedMetricsFile = new Path(fileName);
                src = new Path(fileName + ".working");
            }

            if (!fs.exists(finishedMetricsFile))
                break;
            // delete src - it will be recreated by while statement
            if (fs.delete(src, false)) {
                log.warn("unable to delete metrics file ({})", src);
            }
        }

        try (final SequenceFile.Writer writer = SequenceFile.createWriter(new Configuration(), createSequenceFileWriterOptions(src))) {
            writer.append(new Text(baseName), counters);
        }

        if (!fs.rename(src, finishedMetricsFile))
            log.error("Could not rename metrics file to completed name. Failed file will persist until manually removed.");
    }

    private SequenceFile.Writer.Option[] createSequenceFileWriterOptions(Path src) {
        SequenceFile.Writer.Option keyOption = SequenceFile.Writer.keyClass(Text.class);
        SequenceFile.Writer.Option valueOption = SequenceFile.Writer.valueClass(Counters.class);
        SequenceFile.Writer.Option compressionOption = getCompressionOption();
        SequenceFile.Writer.Option fileOption = SequenceFile.Writer.file(src);
        return new SequenceFile.Writer.Option[] {fileOption, keyOption, valueOption, compressionOption};
    }

    @VisibleForTesting
    SequenceFile.Writer.Option getCompressionOption() {
        return SequenceFile.Writer.compression(ct, cc);
    }
}
