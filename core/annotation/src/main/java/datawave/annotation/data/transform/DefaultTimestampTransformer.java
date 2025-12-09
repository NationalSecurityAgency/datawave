package datawave.annotation.data.transform;

import static datawave.annotation.data.transform.MetadataTransformerBase.maybeMergeSingleValue;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.Set;

public class DefaultTimestampTransformer implements TimestampTransformer {

    public static final String CREATED_DATE_METADATA_KEY = "created_date";
    public static final Set<String> TIMESTAMP_FIELDS = Set.of(CREATED_DATE_METADATA_KEY);

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
    public Long fromMetadataMap(Map<String,String> metadataMap) throws AnnotationTransformException {
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
     *             if the timestamp can't be parsed into zulu time format.
     */
    @Override
    public Map<String,String> toMetadataMap(Long timestamp) throws AnnotationTransformException {
        try {
            String iso8601 = Instant.ofEpochMilli(timestamp).toString();
            return Map.of(CREATED_DATE_METADATA_KEY, iso8601);
        } catch (Exception e) {
            throw new AnnotationTransformException("Exception transforming timestamp to metadata map");
        }
    }

    /**
     * Return the fields that are used to calculate the timestamp.
     *
     * @return a set of timestamp field names.
     */
    @Override
    public Set<String> getMetadataFields() {
        return TIMESTAMP_FIELDS;
    }

    @Override
    public Map<String,String> mergeMetadataMaps(Map<String,String> first, Map<String,String> second) {
        Map<String,String> result = maybeMergeSingleValue(CREATED_DATE_METADATA_KEY, first, second);
        if (result == null) {
            final String firstValue = first.get(CREATED_DATE_METADATA_KEY);
            final String secondValue = second.get(CREATED_DATE_METADATA_KEY);
            final long firstTime = Instant.parse(firstValue).toEpochMilli();
            final long secondTime = Instant.parse(secondValue).toEpochMilli();
            result = Map.of(CREATED_DATE_METADATA_KEY, firstTime > secondTime ? firstValue : secondValue);
        }
        return result;
    }
}
