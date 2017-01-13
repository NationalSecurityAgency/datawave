package nsa.datawave.poller.manager.io;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TranslatingInputFormat<K1,V1,K2,V2> extends InputFormat<K2,V2> {
    public static final String TRR_NAME = "translating.record.reader";
    public static final String WIF_NAME = "wrapped.input.format";
    public static final String SKIP_COUNT = "nsa.datawave.poller.manager.io.TranslatingInputFormat.skipCount";
    
    protected TranslatingRecordReader<K1,V1,K2,V2> trr;
    protected InputFormat<K1,V1> wrappedIf;
    
    public TranslatingInputFormat() {}
    
    @SuppressWarnings({"rawtypes"})
    public static void setTRR(Configuration config, Class<? extends TranslatingRecordReader> c) {
        config.setClass(TRR_NAME, c, TranslatingRecordReader.class);
    }
    
    @SuppressWarnings({"rawtypes"})
    public static void setWIF(Configuration config, Class<? extends InputFormat> c) {
        config.setClass(WIF_NAME, c, InputFormat.class);
    }
    
    @SuppressWarnings("unchecked")
    private TranslatingRecordReader<K1,V1,K2,V2> getTRRInstance(Configuration config) throws IOException {
        try {
            Class<?> cls = config.getClass(TRR_NAME, null, TranslatingRecordReader.class);
            if (cls == null)
                throw new IOException(TRR_NAME + " not set");
            return (TranslatingRecordReader<K1,V1,K2,V2>) cls.newInstance();
        } catch (InstantiationException e) {
            throw new IOException("couldn't instantiate " + TRR_NAME);
        } catch (IllegalAccessException e) {
            throw new IOException("couldn't access " + TRR_NAME);
        }
    }
    
    private InputFormat<K1,V1> getWIFInstance(Configuration config) throws IOException {
        try {
            @SuppressWarnings("unchecked")
            Class<? extends InputFormat<K1,V1>> cls = (Class<? extends InputFormat<K1,V1>>) config.getClass(WIF_NAME, null, InputFormat.class);
            if (cls == null)
                throw new IOException(WIF_NAME + " not set");
            InputFormat<K1,V1> inputFormat = cls.newInstance();
            if (inputFormat instanceof Configurable) {
                ((Configurable) inputFormat).setConf(config);
            }
            return inputFormat;
        } catch (InstantiationException e) {
            throw new IOException("couldn't instantiate " + WIF_NAME);
        } catch (IllegalAccessException e) {
            throw new IOException("couldn't access " + WIF_NAME);
        }
    }
    
    public void setup(JobContext context) {
        try {
            if (this.trr == null) {
                this.trr = getTRRInstance(context.getConfiguration());
                this.trr.setSkipCount(context.getConfiguration().getLong(SKIP_COUNT, 0));
                this.trr.setup(context);
            }
            if (this.wrappedIf == null)
                this.wrappedIf = getWIFInstance(context.getConfiguration());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    @Override
    public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
        setup(context);
        return this.wrappedIf.getSplits(context);
    }
    
    @Override
    public RecordReader<K2,V2> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        setup(context);
        trr.setSource(this.wrappedIf.createRecordReader(split, context));
        return trr;
    }
    
    /**
     * Tells this input format to skip records. For example, if the skip count is set to {@code 1} then the record reader will read one record, skip one record,
     * and then call translate on the read record. If the skip count is set to {@code 10}, the record reader will read one record, skip 10 records, and then
     * call translated on the read record.
     * 
     * @param skipCount
     *            the number of records to skip after reading each record
     * @param config
     *            the current job configuration
     */
    public static void setSkipCount(long skipCount, Configuration config) {
        config.setLong(SKIP_COUNT, skipCount);
    }
}
