package datawave.ingest.data.config.ingest;

import org.junit.Test;

public class ErrorShardedIngestHelperTest {

    /**
     *     private boolean hasErrorIndexConfig(){
     *         return !super.errorIndexedFields.isEmpty();
     *     }
     *
     *     private boolean hasErrorReverseIndexConfig(){
     *         return !super.errorReverseIndexedFields.isEmpty();
     *
     *     }
     *
     *     @Override
     *     public boolean isIndexedField(String fieldName) {
     *         if(hasErrorIndexConfig()) {
     *             return super.isErrorIndexedField(fieldName);
     *         } else {
     *             return super.isIndexedField(fieldName);
     *         }
     *     }
     *
     *     @Override
     *     public boolean isReverseIndexedField(String fieldName) {
     *         if(hasErrorReverseIndexConfig()) {
     *             return super.isErrorReverseIndexedField(fieldName);
     *         } else {
     *             return super.isIndexedField(fieldName);
     *         }
     *     }
     */

    @Test
    public void testHasErrorIndexConfig(){
        ErrorShardedIngestHelper helper = new ErrorShardedIngestHelper();
    }
}