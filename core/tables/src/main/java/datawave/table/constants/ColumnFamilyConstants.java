package datawave.table.constants;

import org.apache.hadoop.io.Text;

/**
 * Constants for Accumulo ColumnFamilies reserved by DataWave
 */
public class ColumnFamilyConstants {

    public static final String TERM_FREQUENCY = "tf";
    public static final String FULL_CONTENT = "d";

    public static final Text TERM_FREQUENCY_TEXT = new Text(TERM_FREQUENCY);
    public static final Text FULL_CONTENT_TEXT = new Text(FULL_CONTENT);

    private ColumnFamilyConstants() {
        // enforce static access
    }
}
