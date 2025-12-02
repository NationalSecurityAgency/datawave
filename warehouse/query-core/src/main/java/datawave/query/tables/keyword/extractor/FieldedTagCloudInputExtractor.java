package datawave.query.tables.keyword.extractor;

import static datawave.query.jexl.JexlASTHelper.deconstructIdentifier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.log4j.Logger;

import datawave.query.attributes.Attribute;
import datawave.query.tables.keyword.KeywordQueryUtil;
import datawave.query.tables.keyword.transform.TagCloudInputTransformer;
import datawave.query.tables.keyword.transform.TagCloudPartitionTransformer;
import datawave.util.keyword.TagCloudInput;
import datawave.util.keyword.TagCloudPartition;

public class FieldedTagCloudInputExtractor implements TagCloudInputExtractor {
    private static final Logger log = Logger.getLogger(FieldedTagCloudInputExtractor.class);
    private static final String DEFAULT_SCORE = "1.0";
    private static final double DEFAULT_MIN_SCORE = .6;

    private String category;
    private List<String> fields;
    private Map<String,String> fieldToScoreField = new HashMap<>();
    // TODO-crwill9 ehh
    private String defaultScore = DEFAULT_SCORE;
    private double minScore = DEFAULT_MIN_SCORE;
    private TagCloudPartition partition;

    public FieldedTagCloudInputExtractor() {}

    @Override
    public void extract(Key source, Map<String,Attribute<? extends Comparable<?>>> documentData) {
        String docId = getDocId(source);

        Map<String,Double> extractedFields = new HashMap<>();
        for (Map.Entry<String,Attribute<? extends Comparable<?>>> data : documentData.entrySet()) {
            String baseField = deconstructIdentifier(data.getKey());
            String group = null;
            if (this.fields.contains(baseField)) {
                int dotIndex = data.getKey().indexOf(".");
                if (dotIndex > -1) {
                    group = data.getKey().substring(dotIndex + 1);
                }

                // default score
                String score = this.defaultScore;
                try {
                    score = getScoreField(baseField, group, documentData);
                } catch (TagCloudInputExtractorException e) {
                    log.warn("failed to extract score for field: " + baseField + " on doc: " + docId + " Defaulting score: " + score, e);
                }

                if (Double.parseDouble(score) >= this.minScore) {
                    List<String> values = KeywordQueryUtil.getStringValuesFromAttribute(data.getValue());
                    for (String value : values) {
                        extractedFields.put(value, Double.parseDouble(score));
                    }
                } else if (log.isDebugEnabled()) {
                    log.debug("excluding " + baseField + " " + score + " < " + minScore + " threshold for doc " + docId);
                }
            }
        }

        TagCloudInput tagCloudInput = new TagCloudInput(docId, source.getColumnVisibility().toString(), extractedFields, Map.of("type", category));

        if (this.partition == null) {
            this.partition = new TagCloudPartition(this.category, this.category, TagCloudPartition.ScoreType.HIGHER_IS_BETTER, new ArrayList<>());
        }

        this.partition.addInput(tagCloudInput);
    }

    @Override
    public TagCloudPartition get() {
        return partition;
    }

    @Override
    public void clear() {
        partition = null;
    }

    @Override
    public String getName() {
        return category;
    }

    @Override
    public TagCloudInputTransformer<TagCloudPartition> getInputTransformer() {
        return TagCloudPartitionTransformer.getInstance();
    }

    /**
     * a score field is presumed to be the same group, but with the score field name
     *
     * @param baseField
     * @param group
     * @param documentData
     * @return
     * @throws TagCloudInputExtractorException
     */
    private String getScoreField(String baseField, String group, Map<String,Attribute<? extends Comparable<?>>> documentData)
                    throws TagCloudInputExtractorException {
        String scoreField = this.fieldToScoreField.get(baseField);
        if (scoreField == null) {
            return this.defaultScore;
        }

        for (Map.Entry<String,Attribute<? extends Comparable<?>>> data : documentData.entrySet()) {
            // exact match for field
            if ((group == null && data.getKey().equals(scoreField)) || (group != null && data.getKey().equals(scoreField + "." + group))) {
                List<String> values = KeywordQueryUtil.getStringValuesFromAttribute(data.getValue());
                if (values.size() == 1) {
                    return values.get(0);
                }
            }
        }

        throw new TagCloudInputExtractorException("Unexpected number of values for field: " + baseField + " score field: " + scoreField + "." + group);
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getCategory() {
        return category;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    public List<String> getFields() {
        return fields;
    }

    /**
     * Get all fields needed by this extractor to work correctly. This will include the fields any required scoring fields
     *
     * @return
     */
    public List<String> getNeededFields() {
        Set<String> neededFields = fields != null ? new HashSet<>(fields) : new HashSet<>();
        if (fieldToScoreField != null) {
            neededFields.addAll(fieldToScoreField.values());
        }

        return new ArrayList<>(neededFields);
    }

    public void setFieldToScoreField(Map<String,String> fieldToScoreField) {
        this.fieldToScoreField = fieldToScoreField;
    }

    public Map<String,String> getFieldToScoreField() {
        return fieldToScoreField;
    }

    public void setDefaultScore(String defaultScore) {
        this.defaultScore = defaultScore;
    }

    public String getDefaultScore() {
        return defaultScore;
    }

    public void setMinScore(double minScore) {
        this.minScore = minScore;
    }

    public double getMinScore() {
        return minScore;
    }
}
