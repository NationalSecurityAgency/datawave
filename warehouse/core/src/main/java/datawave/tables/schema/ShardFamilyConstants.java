package datawave.tables.schema;

import org.apache.hadoop.io.Text;

public class ShardFamilyConstants {
    
    /**
     * Document column
     */
    public static final String DOCUMENT = "d";
    public static final Text DOCUMENT_TXT = new Text(DOCUMENT);
    public static final byte[] DOCUMENT_BYTES = DOCUMENT_TXT.getBytes();
    // locality group name
    public static final String FULL_CONTENT_LOCALITY_NAME = "fullcontent";
    
    /**
     * Term frequency column.
     */
    public static final String TF = "tf";
    public static final Text TF_TXT = new Text(TF);
    public static final byte[] TF_BYTES = TF_TXT.getBytes();
    // locality group name
    /* TODO Make a clearer definition of full content indexers */
    public static final String TERM_FREQUENCY_LOCALITY_NAME = "termfrequency";
    
    /**
     * Fi column
     */
    public static final String FI = "fi";
    public static final Text FI_TXT = new Text(FI);
    public static final byte[] FI_BYTES = FI_TXT.getBytes();
    
}
