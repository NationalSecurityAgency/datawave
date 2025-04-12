package datawave.webservice.annotation;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AnnotationManagerUtil {

    private static final Logger log = LoggerFactory.getLogger(AnnotationManagerUtil.class);

    public RawRecordReference findUid(String field, String value) {
        if (field.equals("DOCUMENT")) {
            final String[] parts = value.split("/");
            if (parts.length != 3) {
                throw new IllegalArgumentException("DOCUMENT reference does not specify all needed parts: " + value
                        + ". value should be in the form 'DOCUMENT:shardId/datatype/eventUID'.");
            }
            // Extract the relevant parts of the value and use them to build a content Range
            else {
                // Get the info necessary to build a content Range
                final String shardId = parts[0];
                final String datatype = parts[1];
                final String uid = parts[2];

                log.debug("Received pieces: {}, {}, {}", shardId, datatype, uid);

                return new RawRecordReference(shardId, datatype, uid);
            }
        }

        log.debug("Could not find UID for field: '{}', value: '{}'", field, value);
        return null;
    }


    public static class RawRecordReference {
        private final String shard;
        private final String dataType;
        private final String uid;

        public RawRecordReference(String shard, String dataType, String uid) {
            this.shard = shard;
            this.dataType = dataType;
            this.uid = uid;
        }

        public String getDataType() {
            return dataType;
        }

        public String getShard() {
            return shard;
        }

        public String getUid() {
            return uid;
        }
    }
}
