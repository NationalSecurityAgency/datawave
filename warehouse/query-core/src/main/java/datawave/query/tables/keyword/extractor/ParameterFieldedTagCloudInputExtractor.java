package datawave.query.tables.keyword.extractor;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import datawave.microservice.query.Query;

/**
 * Works identically to the ParameterFieldedTagCloudInputExtractor except can be configured with query parameters
 */
public class ParameterFieldedTagCloudInputExtractor extends FieldedTagCloudInputExtractor {
    public static final String CATEGORY = "tag.cloud.user.category";
    public static final String FIELDS = "tag.cloud.user.fields";
    public static final String FIELDS_TO_SCORE_FIELDS = "tag.cloud.user.score.fields.map";
    public static final String DEFAULT_SCORE = "tag.cloud.user.score.default";

    @Override
    public void initialize(Query settings) {
        String category = settings.findParameter(CATEGORY).getParameterValue();
        setCategory(category != null ? category.trim() : "");

        String fields = settings.findParameter(FIELDS).getParameterValue();
        if (fields != null) {
            String[] fieldSplits = fields.split(",");
            setFields(Arrays.stream(fieldSplits).map(String::trim).collect(Collectors.toList()));
        }

        String scoreFields = settings.findParameter(FIELDS_TO_SCORE_FIELDS).getParameterValue();
        if (scoreFields != null) {
            String[] splits = scoreFields.split(",");
            Map<String,String> scoreFieldMap = new HashMap<>();
            for (String candidate : splits) {
                String[] kvSplits = candidate.split("=");
                if (kvSplits.length != 2) {
                    throw new IllegalArgumentException(FIELDS_TO_SCORE_FIELDS + " malformed, field1=score_field1,field2=score_field2,field3=score_field3");
                }
                scoreFieldMap.put(kvSplits[0], kvSplits[1]);
            }
            setFieldToScoreField(scoreFieldMap);
        }

        String defaultScore = settings.findParameter(DEFAULT_SCORE).getParameterValue();
        if (defaultScore != null) {
            setDefaultScore(defaultScore);
        }
    }
}
