package datawave.metrics.mapreduce;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;

import datawave.ingest.metric.IngestInput;
import datawave.ingest.metric.IngestOutput;
import datawave.ingest.metric.IngestProcess;
import datawave.metrics.mapreduce.util.TypeNameConverter;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.log4j.Logger;

/**
 * A map task to import Bulk Ingest metrics. Given a Counters object from a Bulk Ingest job, this mapper will format the data into a table structure similar to:
 * <p>
 *
 * <pre>
 * {@code
 * 
 *  Row         Colf                            Colq                   Value
 * -----------------------------------------------------------------------------------
 * |end_time    |[event_type\0count\0duration]  |[job_id(\0out_dir)?]  |<array writable of input files>
 * |job_id      |out_dir                        |                      |<serialized job counters file>
 * }
 * </pre>
 * <p>
 * There are no values stored- all information is a part of the key (row, column family, column qualifier).
 *
 */
public class IngestMetricsMapper extends Mapper<Text,Counters,Text,Mutation> {
    
    private static final char nul = '\000';
    private static final byte[] nulArray = {0x00};
    private static final Text[] emptyTextArray = new Text[0];
    private static final Logger log = Logger.getLogger(IngestMetricsMapper.class);
    
    // TODO: Enable factory injection for any desired TypeNameConverter instance or subclass
    private static final TypeNameConverter typeNameConverter = new TypeNameConverter();
    
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        InputSplit split = context.getInputSplit();
        if (split instanceof FileSplit) {
            FileSplit fsplit = (FileSplit) split;
        }
        super.setup(context);
    }
    
    @Override
    @SuppressWarnings("deprecation")
    public void map(Text jobId, Counters counters, Context context) throws IOException, InterruptedException {
        
        long endTime = counters.findCounter(IngestProcess.END_TIME).getValue();
        long startTime = counters.findCounter(IngestProcess.START_TIME).getValue();
        long duration = endTime - startTime;
        
        // build a map of types to input files
        HashMap<String,Collection<Text>> typeInputFiles = new HashMap<>();
        CounterGroup inFiles = counters.getGroup(IngestInput.FILE_NAME.name());
        for (Counter c : inFiles) {
            String fName = extractFileName(c.getName());
            if (fName == null) {
                continue;
            }
            
            String type;
            try {
                type = typeNameConverter.convertRawFileTransformerToIngest(fName.substring(0, fName.indexOf('_')));
                type = stripRawFileTransformerNumber(type);
            } catch (StringIndexOutOfBoundsException e) {
                // From the normal ingest process, we shouldn't get here. However, when
                // forcing test data through, sometime a file may be incorrectly named,
                // and a '_' will not be found and we'll get here. It's probably best to
                // just skip it.
                continue;
            }
            if (typeInputFiles.containsKey(type)) {
                typeInputFiles.get(type).add(new Text(fName));
            } else {
                LinkedList<Text> fnames = new LinkedList<>();
                fnames.add(new Text(fName));
                typeInputFiles.put(type, fnames);
            }
        }
        
        // build a collection of tuples [type:count:duration] to use as part of the
        // key
        CounterGroup events = counters.getGroup(IngestOutput.EVENTS_PROCESSED.name());
        HashMap<String,Text> typeTuples = new HashMap<>();
        for (Counter c : events) {
            StringBuilder tuple = new StringBuilder();
            String type = c.getName();
            tuple.append(type);
            tuple.append(nul);
            tuple.append(c.getValue()); // value is the number of events
            tuple.append(nul);
            tuple.append(duration);
            typeTuples.put(type, new Text(tuple.toString()));
        }
        
        Text colQ = new Text(jobId);
        String outputDirectory = null;
        if (counters.findCounter(IngestProcess.LIVE_INGEST).getValue() != 1) {
            // if we aren't live, we only have 1 output directory in the
            // OUTPUT_DIRECTORY group
            outputDirectory = counters.getGroup(IngestProcess.OUTPUT_DIRECTORY.name()).iterator().next().getName();
            Text outDir = new Text(outputDirectory);
            colQ.append(nulArray, 0, nulArray.length);
            colQ.append(outDir.getBytes(), 0, outDir.getLength());
        }
        
        // creates timeseries index and updates the datatype index
        if (!typeTuples.isEmpty()) {
            Mutation mut = new Mutation(Long.toString(endTime));
            Mutation dataTypeMutation = new Mutation("datatype");
            for (Entry<String,Text> tuple : typeTuples.entrySet()) {
                Collection<Text> inputFiles = typeInputFiles.get(tuple.getKey());
                ArrayWritable value = new ArrayWritable(Text.class);
                if (inputFiles != null && !inputFiles.isEmpty()) {
                    Text[] typeIFs = inputFiles.toArray(new Text[inputFiles.size()]);
                    value.set(typeIFs);
                } else {
                    value.set(emptyTextArray);
                }
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                value.write(new DataOutputStream(baos));
                mut.put(tuple.getValue(), colQ, new Value(baos.toByteArray()));
                dataTypeMutation.put(tuple.getKey(), "", "");
            }
            context.write(null, mut);
            context.write(null, dataTypeMutation);
        }
        
        // create the mutation that stores the counters files
        Mutation statsPersist = new Mutation("jobId\u0000" + jobId);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        counters.write(new DataOutputStream(baos));
        statsPersist.put(outputDirectory == null ? "" : outputDirectory, "", new Value(baos.toByteArray()));
        context.write(null, statsPersist);
        
    }
    
    /**
     * Makes two attempts to parse a path. If the path can be parsed using the Hadoop <code>Path</code> class, then this method will create a new
     * <code>Path</code> object using the supplied path and return the value of <code>Path.getName()</code>.
     *
     * Should that fail, this method try to locate the last '/' character and return the substring starting after the last '/' character.
     *
     * If there is no '/' character, then null is returned.
     *
     * @param path
     *            the file path
     * @return a file name
     */
    public static String extractFileName(String path) {
        // first see if we have a full path
        try {
            Path p = new Path(path);
            return p.getName();
        } catch (Exception e) {/* this is ok-- we can continue */}
        
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash == -1) {
            return null;
        } else {
            return path.substring(lastSlash + 1);
        }
    }
    
    /**
     * We can have multiple transformer processes per datatype, each returning a file with the prefix {datatype}.{transformerNumber}. This method returns just
     * the {datatype} portion.
     * 
     * @param type
     *            the datatype
     * @return the datatype string
     */
    public static String stripRawFileTransformerNumber(String type) {
        final int wheresTheDot = type.indexOf('.');
        if (wheresTheDot == -1) {
            return type;
        } else {
            return type.substring(0, wheresTheDot);
        }
    }
}
