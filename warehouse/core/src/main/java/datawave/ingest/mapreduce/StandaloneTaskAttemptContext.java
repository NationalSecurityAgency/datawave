package datawave.ingest.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.TaskInputOutputContextImpl;

public class StandaloneTaskAttemptContext<KEYIN,VALUEIN,KEYOUT,VALUEOUT> extends TaskInputOutputContextImpl<KEYIN,VALUEIN,KEYOUT,VALUEOUT> {
    private final StandaloneStatusReporter reporter;

    public StandaloneTaskAttemptContext(Configuration conf, StandaloneStatusReporter reporter) {
        super(conf, new TaskAttemptID(), null, null, reporter);
        this.reporter = reporter;
    }

    public long putIfAbsent(final Enum<?> cName, final long value) {
        final Counter counter = getCounter(cName);
        final long origValue = counter.getValue();

        if (origValue == 0)
            counter.increment(value);
        return origValue;
    }

    public StandaloneStatusReporter getReporter() {
        return reporter;
    }

    @Override
    public KEYIN getCurrentKey() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public VALUEIN getCurrentValue() throws IOException, InterruptedException {
        return null;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        return false;
    }
}
