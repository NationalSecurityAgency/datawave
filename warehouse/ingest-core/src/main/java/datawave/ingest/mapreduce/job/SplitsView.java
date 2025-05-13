package datawave.ingest.mapreduce.job;

import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.Iterator;

public interface SplitsView {
    boolean isEmpty() throws IOException;
    Iterator<SplitEntry> getSplits(String tableName) throws IOException;
    SplitEntry lookupSplit(String tableName, Text row) throws IOException;

}
