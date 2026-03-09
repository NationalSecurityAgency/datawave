package datawave.ingest.mapreduce.job;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.file.rfile.RFile;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import datawave.util.accumulo.RFileUtil;

/**
 * Mapper for the RFile split coverage analysis job. Each map task receives one RFile (via {@link RFileInputFormat}). Rather than iterating through all
 * key/value pairs, this mapper opens the RFile directly to extract the first and last key rows, then binary searches the table splits to determine how many
 * split points the file spans.
 *
 * <p>
 * Output is a single tab-delimited text line per RFile: {@code filename \t firstRow \t lastRow \t splitsCovered \t percentSplitsCovered}
 */
public class RFileSplitCoverageMapper extends Mapper<Key,Value,NullWritable,Text> {

    private static final Logger log = Logger.getLogger(RFileSplitCoverageMapper.class);

    public static final String SPLITS_CONFIG_KEY = "rfile.split.coverage.splits";

    private List<Text> splits;
    private final Text outputValue = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        splits = deserializeSplits(conf.get(SPLITS_CONFIG_KEY, ""));
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        setup(context);

        // extract the file path from the input split
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path filePath = fileSplit.getPath();
        Configuration conf = context.getConfiguration();

        try (RFile.Reader reader = RFileUtil.getRFileReader(conf, filePath)) {
            Key firstKey = reader.getFirstKey();
            Key lastKey = reader.getLastKey();

            if (firstKey == null || lastKey == null) {
                log.warn("Empty RFile: " + filePath);
                outputValue.set(filePath.getName() + "\t<empty>\t<empty>\t0\t0.00");
                context.write(NullWritable.get(), outputValue);
                return;
            }

            Text firstRow = firstKey.getRow();
            Text lastRow = lastKey.getRow();

            int splitsCovered = computeSplitsCovered(firstRow, lastRow);
            double pct = splits.isEmpty() ? 0.0 : (splitsCovered * 100.0) / splits.size();

            outputValue.set(String.format("%s\t%s\t%s\t%d\t%.2f", filePath.getName(), firstRow, lastRow, splitsCovered, pct));
            context.write(NullWritable.get(), outputValue);
        }

        cleanup(context);
    }

    /**
     * Compute the number of split points that fall within the row range [firstRow, lastRow]. A split point s is "covered" if firstRow &lt;= s &lt;= lastRow.
     *
     * @param firstRow
     *            the first row in the RFile
     * @param lastRow
     *            the last row in the RFile
     * @return the number of split points between firstRow and lastRow (inclusive)
     */
    int computeSplitsCovered(Text firstRow, Text lastRow) {
        if (splits.isEmpty()) {
            return 0;
        }

        // find the index of the first split >= firstRow
        int startIdx = Collections.binarySearch(splits, firstRow);
        if (startIdx < 0) {
            startIdx = -(startIdx + 1);
        }

        // find the index of the last split <= lastRow
        int endIdx = Collections.binarySearch(splits, lastRow);
        if (endIdx < 0) {
            endIdx = -(endIdx + 1) - 1;
        }

        return Math.max(0, endIdx - startIdx + 1);
    }

    static List<Text> deserializeSplits(String encoded) {
        if (encoded == null || encoded.isEmpty()) {
            return Collections.emptyList();
        }
        String[] parts = encoded.split(",");
        List<Text> result = new ArrayList<>(parts.length);
        for (String part : parts) {
            result.add(new Text(Base64.decodeBase64(part)));
        }
        return result;
    }

    static String serializeSplits(List<Text> splits) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < splits.size(); i++) {
            if (i > 0) {
                sb.append(',');
            }
            sb.append(Base64.encodeBase64String(splits.get(i).copyBytes()));
        }
        return sb.toString();
    }
}
