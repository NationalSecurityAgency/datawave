package datawave.mapreduce.shardStats;

import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.Text;

public abstract class StatsInit {
    
    // constants for key visibility and timestamp
    static final ColumnVisibility TEST_VISIBILITY = new ColumnVisibility("vis");
    
    static final Value EMPTY_VALUE = new Value(new byte[0]);
    static final String NUL_SEPERATOR = "\u0000";
    static final String TEST_TABLE = "test";
    static final Text TABLE = new Text(TEST_TABLE);
}
