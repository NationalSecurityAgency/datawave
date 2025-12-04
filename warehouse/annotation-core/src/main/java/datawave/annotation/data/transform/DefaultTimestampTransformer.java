package datawave.annotation.data.transform;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;

public class DefaultTimestampTransformer implements TimestampTransformer {

    public static final String CREATED_DATE_METADATA_KEY = "created_date";

    /**
     * Finds the @{code CREATED_DATE_METADATA_KEY} in the metadataMap in ISO8601 zulu and transforms it into the number of milliseconds since the epoch.
     *
     * @param metadataMap
     *            a map containing metadata to read
     * @return the number of milliseconds since the epoch, 0 if the expected timestamp was not present.
     * @throws AnnotationTransformException
     *             if the created_date key is present but can't be parsed.
     */
    @Override
    public long toTimestamp(Map<String,String> metadataMap) throws AnnotationTransformException {
        try {
            String createdDateString = metadataMap.get(CREATED_DATE_METADATA_KEY);
            return Instant.parse(createdDateString).toEpochMilli();
        } catch (DateTimeParseException | NullPointerException e) {
            throw new AnnotationTransformException("Exception parsing date for timestamp", e);
        }
    }

    /**
     * Transforms the number of milliseconds since the epoch into ISO8601 zulu date format.
     *
     * @param timestamp
     *            the number of milliseconds since the epoch.
     * @return a map containing the @{code CREATE_DATE_METADATA_KEY}
     * @throws AnnotationTransformException
     *             if the timestamp can't be parsed into zulo tade format.
     */
    @Override
    public Map<String,String> toMetadataMap(long timestamp) throws AnnotationTransformException {
        try {
            String iso8601 = Instant.ofEpochMilli(timestamp).toString();
            return Map.of(CREATED_DATE_METADATA_KEY, iso8601);
        } catch (Exception e) {
            throw new AnnotationTransformException("Exception transforming timestamp to metadata map");
        }
    }
}
