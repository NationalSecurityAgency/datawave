package datawave.ingest.data.config.ingest;

import org.junit.Test;

public class ErrorShardedIngestHelperTest {

    /*
     * SETH NOTE What kind of test coverage will be needed for this outside of basic method tests? Are there any examples to reference?
     */

    /*
     * private boolean hasErrorIndexConfig(){ return !super.errorIndexedFields.isEmpty(); }
     *
     * private boolean hasErrorReverseIndexConfig(){ return !super.errorReverseIndexedFields.isEmpty();
     *
     * }
     *
     * @Override public boolean isIndexedField(String fieldName) { if(hasErrorIndexConfig()) { return super.isErrorIndexedField(fieldName); } else { return
     *           super.isIndexedField(fieldName); } }
     *
     * @Override public boolean isReverseIndexedField(String fieldName) { if(hasErrorReverseIndexConfig()) { return super.isErrorReverseIndexedField(fieldName);
     *           } else { return super.isIndexedField(fieldName); } }
     */

    /*
     * Test that
     */
    @Test
    public void testHasErrorIndexConfig() {
        ErrorShardedIngestHelper helper = new ErrorShardedIngestHelper();
    }
}
