package datawave.ingest.mapreduce.job;

import org.apache.hadoop.io.Text;

public interface SplitEntry {

    int getOffset();

    boolean hasLocation();

    String getLocation();

    Text getSplit();
}
