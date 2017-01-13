package nsa.datawave.poller.manager.io;

import java.io.IOException;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class TranslatingOutputFormat<K1,V1,K2,V2> extends OutputFormat<K1,V1> {
    public static final String TRW_NAME = "translating.record.writer";
    public static final String WOF_NAME = "wrapped.output.format";
    
    private TranslatingRecordWriter<K1,V1,K2,V2> trw;
    private OutputFormat<K2,V2> wrappedOf;
    
    public TranslatingOutputFormat() {}
    
    @SuppressWarnings("rawtypes")
    public static void setTRW(JobContext context, Class<? extends TranslatingRecordWriter> c) {
        context.getConfiguration().setClass(TRW_NAME, c, TranslatingRecordWriter.class);
    }
    
    @SuppressWarnings("rawtypes")
    public static void setWOF(JobContext context, Class<? extends OutputFormat> c) {
        context.getConfiguration().setClass(WOF_NAME, c, OutputFormat.class);
    }
    
    private OutputFormat<K2,V2> getWOFInstance(JobContext context) throws IOException {
        try {
            @SuppressWarnings("unchecked")
            Class<? extends OutputFormat<K2,V2>> cls = (Class<? extends OutputFormat<K2,V2>>) context.getConfiguration().getClass(WOF_NAME, null,
                            OutputFormat.class);
            if (cls == null)
                throw new IOException(WOF_NAME + " not set");
            OutputFormat<K2,V2> outputFormat = cls.newInstance();
            if (outputFormat instanceof Configurable) {
                ((Configurable) outputFormat).setConf(context.getConfiguration());
            }
            return outputFormat;
        } catch (Exception e) {
            throw new IOException("couldn't instantiate " + WOF_NAME + ": " + e.getMessage());
        }
    }
    
    @SuppressWarnings("unchecked")
    private TranslatingRecordWriter<K1,V1,K2,V2> getTRWInstance(JobContext context) throws IOException {
        try {
            Class<?> cls = context.getConfiguration().getClass(TRW_NAME, null, TranslatingRecordWriter.class);
            if (cls == null)
                throw new Exception(TRW_NAME + " not set");
            return (TranslatingRecordWriter<K1,V1,K2,V2>) cls.newInstance();
        } catch (Exception e) {
            throw new IOException("couldn't instantiate " + TRW_NAME + ": " + e.getMessage());
        }
    }
    
    public void setup(JobContext context) throws IOException {
        if (this.trw == null)
            this.trw = getTRWInstance(context);
        if (this.wrappedOf == null)
            this.wrappedOf = getWOFInstance(context);
    }
    
    @Override
    public RecordWriter<K1,V1> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
        setup(context);
        trw.setSink(this.wrappedOf.getRecordWriter(context));
        return trw;
    }
    
    @Override
    public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
        setup(context);
        this.wrappedOf.checkOutputSpecs(context);
    }
    
    @Override
    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (wrappedOf == null)
            this.wrappedOf = getWOFInstance(context);
        return this.wrappedOf.getOutputCommitter(context);
    }
    
}
