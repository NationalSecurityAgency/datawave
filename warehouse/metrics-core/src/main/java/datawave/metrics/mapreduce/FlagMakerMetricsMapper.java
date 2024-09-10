package datawave.metrics.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

import datawave.metrics.util.WritableUtil;
import datawave.metrics.util.flag.FlagFile;
import datawave.metrics.util.flag.InputFile;

/**
 * A map task to import Flag Maker metrics into Accumulo. Given a Counters object from a Bulk Ingest job, this mapper will format the data into a table
 * structure similar to:
 * <p>
 *
 * FlagMakerMetrics table: Row Colf Colq Value ----------------------------------------------------------------------------------- |input file |duration |job
 * end time |{@code <serialized job counters>}
 *
 */
public class FlagMakerMetricsMapper extends Mapper<Text,Counters,Text,Mutation> {

    private static final Logger log = Logger.getLogger(FlagMakerMetricsMapper.class);

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit split = context.getInputSplit();
        System.out.println(split.getClass());
        if (split instanceof FileSplit) {
            FileSplit fsplit = (FileSplit) split;
            System.out.println(fsplit.getPath());
        }
        super.setup(context);
    }

    @Override
    public void map(Text flagFile, Counters counters, Context context) throws IOException, InterruptedException {
        System.out.println("Received counters for job " + flagFile);

        log.info(counters);

        long endTime = counters.findCounter(InputFile.FLAGMAKER_END_TIME).getValue();
        long startTime = counters.findCounter(InputFile.FLAGMAKER_START_TIME).getValue();

        Mutation statsPersist = new Mutation("flagFile\u0000" + flagFile);
        statsPersist.put("", "", new Value(serializeCounters(counters)));
        context.write(null, statsPersist);

        // Breaking it down into individual counters... Can't get individual stats when batch-processing in the FlagMaker
        for (Counter c : counters.getGroup(InputFile.class.getSimpleName())) {
            Text outFile = new Text(c.getName());
            Mutation m = new Mutation(outFile);
            long fileTime = c.getValue();
            try {
                Counters cs = new Counters();
                cs.findCounter(InputFile.class.getSimpleName(), outFile.toString()).setValue(c.getValue());
                cs.findCounter(FlagFile.class.getSimpleName(), flagFile.toString()).increment(1);
                cs.findCounter(InputFile.FLAGMAKER_END_TIME).setValue(endTime);
                cs.findCounter(InputFile.FLAGMAKER_START_TIME).setValue(startTime);
                m.put(WritableUtil.getLong(endTime - fileTime), WritableUtil.getLong(endTime), new Value(serializeCounters(cs)));
                context.write(null, m);
            } catch (IOException e) {
                log.error("Could not add counters to mutation!!!", e);
            }
        }
    }

    public static byte[] serializeCounters(final Counters c) throws IOException {
        if (c == null) {
            throw new IllegalArgumentException("Writable cannot be null");
        }
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(byteStream);
        try {
            c.write(out);
            out.close();
            out = null;
            return byteStream.toByteArray();
        } finally {
            if (out != null) {
                out.close();
            }
        }
    }
}
