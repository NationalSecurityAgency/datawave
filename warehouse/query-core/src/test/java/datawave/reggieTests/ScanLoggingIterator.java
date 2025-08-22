package datawave.reggieTests;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.Filter;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

public class ScanLoggingIterator extends Filter {

    private String hostname;
    private Value topValue;

    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        super.init(source, options, env);
        try {
            this.hostname = InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            this.hostname = "unknown-host";
        }
        System.out.println("ScanLoggingIterator initialized on host: " + hostname);
    }

    @Override
    public boolean accept(Key k, Value v) {
        String threadName = Thread.currentThread().getName();
        System.out.println("Running on thread: " + threadName);

        String server = threadName.contains("scanserver") ? "ScanServer" : "TServer";
        this.topValue = new Value((server + "|" + v.toString()).getBytes());
        // Replace value with one that includes hostname
        // topValue = new Value((hostname + "|" + v.toString()).getBytes());
        return true;
    }

    @Override
    public Value getTopValue() {
        return topValue;
    }

}
