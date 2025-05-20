package datawave.metrics.analytic;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.VLongWritable;
import org.apache.hadoop.io.Writable;

import com.google.common.collect.Lists;

public class FileLatency implements Writable {
    private Collection<Phase> phases;
    private long eventCount;
    private String fileName;
    // lazily initialized durations
    private boolean areDurationsReady = false;
    private long rawFileTransformDuration, delayRawFileTransformToIngest, ingestJobDuration, delayIngestToLoader, loaderDuration, totalLatency;
    private boolean hasLoaderPhase;

    public FileLatency(Collection<Phase> phases, long eventCount, String fileName) {
        this.phases = phases;
        this.eventCount = eventCount;
        this.fileName = fileName;
    }

    public FileLatency() {}

    public Collection<Phase> getPhases() {
        return Collections.unmodifiableCollection(phases);
    }

    public long getEventCount() {
        return eventCount;
    }

    public String getFileName() {
        return fileName;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(getFileName());
        new VLongWritable(getEventCount()).write(out);
        makeWritable(phases, Phase.class).write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        areDurationsReady = false;
        fileName = in.readUTF();
        VLongWritable eventCount = new VLongWritable();
        eventCount.readFields(in);
        this.eventCount = eventCount.get();
        ArrayWritable phases = new ArrayWritable(Phase.class);
        phases.readFields(in);
        this.phases = makeList(phases.get());
    }

    public static ArrayWritable makeWritable(Collection<?> writables, Class<? extends Writable> impl) {
        Writable[] array = new Writable[writables.size()];
        Iterator<?> writable = writables.iterator();
        for (int i = 0; i < array.length; ++i)
            array[i] = (Writable) writable.next();
        ArrayWritable arrayWritable = new ArrayWritable(impl);
        arrayWritable.set(array);
        return arrayWritable;
    }

    @SuppressWarnings("unchecked")
    public static <T extends Writable> List<T> makeList(Writable[] array) {
        return (ArrayList<T>) Lists.newArrayList(array);
    }

    @Override
    public String toString() {
        // Print out the phases.
        final StringBuilder sb = new StringBuilder();

        sb.append(this.fileName).append(", ");
        sb.append(this.eventCount).append(" events, phases:");
        sb.append(this.phases);

        return sb.toString();
    }

    public long getTotalLatency() {
        ensureDurationsAreReady();
        return totalLatency;
    }

    public long getRawFileTransformDuration() {
        ensureDurationsAreReady();
        return rawFileTransformDuration;
    }

    public long getDelayRawFileTransformToIngest() {
        ensureDurationsAreReady();
        return delayRawFileTransformToIngest;
    }

    public long getIngestJobDuration() {
        ensureDurationsAreReady();
        return ingestJobDuration;
    }

    public long getDelayIngestToLoader() {
        ensureDurationsAreReady();
        return delayIngestToLoader;
    }

    public long getLoaderDuration() {
        ensureDurationsAreReady();
        return loaderDuration;
    }

    public boolean hasLoaderPhase() {
        ensureDurationsAreReady();
        return hasLoaderPhase;
    }

    private void ensureDurationsAreReady() {
        if (!areDurationsReady) {
            calculateDurations();
        }
    }

    private void calculateDurations() {
        Iterator<Phase> phaseIterator = this.getPhases().iterator();
        Phase rawFileTransformPhase = phaseIterator.next();
        Phase ingestJobPhase = phaseIterator.next();
        Phase loaderPhase = null;
        // only exists for bulk ingest
        if (phaseIterator.hasNext()) {
            loaderPhase = phaseIterator.next();
        }

        this.hasLoaderPhase = (null != loaderPhase);

        long receiveTime = rawFileTransformPhase.start();
        long rawFileTransformCompletionTime = rawFileTransformPhase.end();
        long ingestJobStartTime = ingestJobPhase.start();
        long ingestJobCompletionTime = ingestJobPhase.end();
        long loadTime;

        if (this.hasLoaderPhase) {
            long loaderStartTime = loaderPhase.start();
            long loaderStopTime = loaderPhase.end();

            this.delayIngestToLoader = loaderStartTime - ingestJobCompletionTime;
            this.loaderDuration = loaderStopTime - loaderStartTime;
            loadTime = loaderStopTime; // for bulk, loadTime = loaderStopTime
        } else {
            this.delayIngestToLoader = 0;
            this.loaderDuration = 0;
            loadTime = ingestJobCompletionTime; // for live, loadTime = ingestJobCompletionTime
        }

        this.totalLatency = loadTime - receiveTime;
        this.rawFileTransformDuration = rawFileTransformCompletionTime - receiveTime;
        this.delayRawFileTransformToIngest = ingestJobStartTime - rawFileTransformCompletionTime;
        this.ingestJobDuration = ingestJobCompletionTime - ingestJobStartTime;

        this.areDurationsReady = true;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (!(obj instanceof FileLatency))
            return false;
        FileLatency other = (FileLatency) obj;

        if (this.hashCode() != other.hashCode())
            return false;
        return Objects.equals(this.eventCount, other.eventCount) && Objects.equals(this.fileName, other.fileName) && Objects.equals(this.phases, other.phases)
                        && Objects.equals(this.getTotalLatency(), other.getTotalLatency())
                        && Objects.equals(this.getRawFileTransformDuration(), other.getRawFileTransformDuration())
                        && Objects.equals(this.getDelayRawFileTransformToIngest(), other.getDelayRawFileTransformToIngest())
                        && Objects.equals(this.getIngestJobDuration(), other.getIngestJobDuration())
                        && Objects.equals(this.getDelayIngestToLoader(), other.getDelayIngestToLoader())
                        && Objects.equals(this.getLoaderDuration(), other.getLoaderDuration());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.eventCount, this.fileName, this.phases, this.getTotalLatency(), this.getRawFileTransformDuration(),
                        this.getDelayRawFileTransformToIngest(), this.getIngestJobDuration(), this.getDelayIngestToLoader(), this.getLoaderDuration());
    }
}
