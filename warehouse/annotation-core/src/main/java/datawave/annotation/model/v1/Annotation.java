package datawave.annotation.model.v1;

import static datawave.annotation.util.Validator.notNullOrEmpty;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import datawave.annotation.protobuf.v1.Segment;
import datawave.annotation.util.Validator;

public class Annotation {

    /** the shard of the annotated document */
    private final String shard;
    /** the data type of the annotated document */
    private final String dataType;
    /** the uid of the annotated document */
    private final String uid;
    /** the type of the annotation */
    private final String annotationType;
    /** the uid of the annotation - must be unique per document */
    private String annotationId;
    /** metadata associated with the annotation */
    private final Map<String,String> metadata;
    /** the segments of the document this annotation is associated with */
    private final List<Segment> segments;

    /**
     *
     * @param shard
     *            the shard of the annotated document
     * @param dataType
     *            the data type of the annotated document
     * @param uid
     *            the uid of the annotated document
     * @param annotationType
     *            the type of the annotation
     * @param annotationId
     *            the uid of the annotation - must be unique per document
     * @param metadata
     *            optional metadata associated with the annotation
     * @param segments
     *            the segments of the document this annotation is associated with
     */
    protected Annotation(String shard, String dataType, String uid, String annotationType, String annotationId, Map<String,String> metadata,
                    List<Segment> segments) {
        this.shard = shard;
        this.dataType = dataType;
        this.uid = uid;
        this.annotationType = annotationType;
        this.annotationId = annotationId;
        this.metadata = metadata;
        this.segments = segments;
    }

    public void generateUID() {
        // TODO fix this - we should use something else to generate the uid.
    }

    public String getAnnotationId() {
        return annotationId;
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

    public String getAnnotationType() {
        return annotationType;
    }

    public Map<String,String> getMetadata() {
        return metadata;
    }

    public List<Segment> getSegments() {
        return segments;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "Annotation{" + "annotationId=" + annotationId + ", shard='" + shard + '\'' + ", dataType='" + dataType + '\'' + ", uid=" + uid
                        + ", annotationType='" + annotationType + '\'' + '}';
    }

    /** Used to build annotations */
    public static class Builder {
        private String shard;
        private String dataType;
        private String uid;
        private String annotationType;
        private String annotationId;

        // what is this and how do we store it?
        private Map<String,String> metadata;

        private List<Segment> segments;

        /** used to validate the values in the shard field, must be in the format 12345_123. */
        private static final Predicate<Annotation> hasValidShard = new Predicate<>() {
            @Override
            public boolean test(Annotation annotation) {
                if (annotation == null) {
                    return false;
                }

                final String shard = annotation.getShard();
                if (notNullOrEmpty(shard)) {
                    String[] parts = shard.split("_");
                    if (parts.length == 2) { // must be only two parts
                        for (String s : parts) { // all parts must be digits.
                            int len = s.length(); // todo: constrain to expected length for each part?
                            for (int i = 0; i < len; i++) {
                                if (!Character.isDigit(s.charAt(i))) {
                                    return false; // found a non digit, reject.
                                }
                            }
                        }
                        return true; // two parts split by '_' and all digits, accept.
                    }
                }
                return false; // reject otherwise
            }
        };

        /** used to validate the entire annotation */
        private static final Validator<Annotation> annotationValidator = Validator.<Annotation> create().addCheck(hasValidShard, "shard must be valid")
                        .addCheck(a -> notNullOrEmpty(a.getDataType()), "datatype must be present")
                        .addCheck(a -> notNullOrEmpty(a.getUid()), "uid must be present")
                        .addCheck(a -> notNullOrEmpty(a.getAnnotationType()), "annotation type must be present");

        protected Builder() {

        }

        public Builder setDataType(String dataType) {
            this.dataType = dataType;
            return this;
        }

        public Builder setShard(String shard) {
            this.shard = shard;
            return this;
        }

        public Builder setUid(String uid) {
            this.uid = uid;
            return this;
        }

        public Builder setAnnotationType(String annotationType) {
            this.annotationType = annotationType;
            return this;
        }

        public Builder setAnnotationId(String annotationId) {
            this.annotationId = annotationId;
            return this;
        }

        public Builder setMetadata(Map<String,String> metadata) {
            this.metadata = metadata;
            return this;
        }

        public Builder putMetadata(String key, String value) {
            if (this.metadata == null) {
                this.metadata = new HashMap<>();
            }
            this.metadata.put(key, value);
            return this;
        }

        public Builder setSegments(List<Segment> segments) {
            this.segments = segments;
            return this;
        }

        public Builder addSegment(Segment segment) {
            if (this.segments == null) {
                this.segments = new ArrayList<>();
            }
            this.segments.add(segment);
            return this;
        }

        public Annotation build() {
            Annotation a = new Annotation(shard, dataType, uid, annotationType, annotationId, metadata, segments);
            Validator.ValidationState<Annotation> as = annotationValidator.check(a);
            if (!as.isValid()) {
                throw new IllegalArgumentException("Annotation is not valid. Errors include: " + as.getErrors());
            }
            if (annotationId == null) {
                a.generateUID();
            }
            return a;
        }
    }
}
