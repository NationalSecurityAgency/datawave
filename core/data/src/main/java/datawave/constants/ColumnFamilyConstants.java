package datawave.constants;

import org.apache.hadoop.io.Text;

/**
 * Constants for Accumulo ColumnFamilies reserved by DataWave
 */
public class ColumnFamilyConstants {

    public static final String TERM_FREQUENCY_NAME = "tf";
    public static final String FULL_CONTENT_NAME = "d";

    public static final Text TERM_FREQUENCY = new Text(TERM_FREQUENCY_NAME);
    public static final Text FULL_CONTENT = new Text(FULL_CONTENT_NAME);

    private ColumnFamilyConstants() {
        // enforce static access
    }
}
