package datawave.query.iterator;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Collection;
import java.util.Map;

import datawave.query.exceptions.LoadAverageWatchException;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.WrappingIterator;

public class LoadAverageWatchIterator extends WrappingIterator {

    private static final String CAPTURE_LOAD_AVERAGE = "CAPTURE_LOAD_AVERAGE";

    private static final String SYSTEM_LOAD_THRESHOLD = "SYSTEM_LOAD_THRESHOLD";

    protected boolean reportHighLoad = false;

    protected double loadThresholdAboveProcs = 2.0;

    protected static final OperatingSystemMXBean OS_BEAN = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);

    public LoadAverageWatchIterator(LoadAverageWatchIterator other, IteratorEnvironment env) {
        this.setSource(other.getSource().deepCopy(env));
        reportHighLoad = other.reportHighLoad;
        loadThresholdAboveProcs = other.loadThresholdAboveProcs;
    }

    public static void setErrorReporting(IteratorSetting cfg) {
        cfg.addOption(CAPTURE_LOAD_AVERAGE, CAPTURE_LOAD_AVERAGE);
    }

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {

        if (null != options.get(CAPTURE_LOAD_AVERAGE)) {
            reportHighLoad = true;
        }

        if (null != options.get(SYSTEM_LOAD_THRESHOLD)) {
            try {
                loadThresholdAboveProcs = Double.valueOf(options.get(SYSTEM_LOAD_THRESHOLD));
            } catch (Exception e) {

            }
        }

        try {

            super.init(source, options, env);

        } catch (RuntimeException | IOException e) {
            throw e;
        }
    }

    protected boolean loadExceedThreshold() {

        if (OS_BEAN.getSystemLoadAverage() / OS_BEAN.getAvailableProcessors() > loadThresholdAboveProcs) {
            return true;
        }
        return false;

    }

    @Override
    public void next() throws IOException {
        if (loadExceedThreshold()) {
            throw new LoadAverageWatchException();
        }
        try {

            super.next();

        } catch (RuntimeException | IOException e) {
            throw e;
        }
    }

    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        if (loadExceedThreshold()) {
            throw new LoadAverageWatchException();
        }
        try {
            super.seek(range, columnFamilies, inclusive);
        } catch (RuntimeException | IOException e) {
            throw e;
        }

    }

    @Override
    public SortedKeyValueIterator<Key,Value> deepCopy(IteratorEnvironment env) {
        return new LoadAverageWatchIterator(this, env);
    }
}
