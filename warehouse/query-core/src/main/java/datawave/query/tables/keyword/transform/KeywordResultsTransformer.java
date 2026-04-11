package datawave.query.tables.keyword.transform;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.util.keyword.KeywordResults;
import datawave.util.keyword.TagCloudInput;
import datawave.util.keyword.TagCloudPartition;

public class KeywordResultsTransformer implements TagCloudInputTransformer<KeywordResults> {
    public static final String LABEL = "keyword";

    private boolean languagePartitioned = true;
    private Map<String,String> identifierMap = new HashMap<>();

    @Override
    public Entry<Key,Value> encode(KeywordResults input) {
        // TODO-crwill9 if we push this all the way into the iterator
        throw new UnsupportedOperationException("TODO");
    }

    @Override
    public boolean canDecode(Entry<Key,Value> input) {
        try {
            return KeywordResults.canDeserialize(input.getValue().get());
        } catch (IOException e) {
            throw new RuntimeException("error checking decoding " + input.getKey(), e);
        }
    }

    @Override
    public TagCloudPartition decode(Entry<Key,Value> input) {
        KeywordResults keywordResults;
        try {
            keywordResults = KeywordResults.deserialize(input.getValue().get());
        } catch (IOException e) {
            throw new RuntimeException("Could not deserialize KeywordResults from K/V " + input.getKey(), e);
        }

        final String identifier = identifierMap.get(keywordResults.getSource());
        if (identifier != null) {
            // update the source from the identifier map.
            keywordResults.setSource(identifier);
        }

        TagCloudInput tagCloudInput = getTagCloudInput(keywordResults);

        String partition = "";
        if (languagePartitioned && !keywordResults.getLanguage().isEmpty()) {
            partition = keywordResults.getLanguage();
        }

        return new TagCloudPartition(partition, LABEL, List.of(tagCloudInput));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }

        if (!(other instanceof KeywordResultsTransformer)) {
            return false;
        }

        KeywordResultsTransformer otherTransformer = (KeywordResultsTransformer) other;
        return Objects.equals(languagePartitioned, otherTransformer.languagePartitioned) && Objects.equals(identifierMap, otherTransformer.identifierMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(languagePartitioned, identifierMap);
    }

    private TagCloudInput getTagCloudInput(KeywordResults keywordResults) {
        Map<String,String> metadata = new HashMap<>();
        metadata.put("type", LABEL);
        if (!keywordResults.getView().isEmpty()) {
            metadata.put("view", keywordResults.getView());
        }
        if (!keywordResults.getLanguage().isEmpty()) {
            metadata.put("language", keywordResults.getLanguage());
        }

        return new TagCloudInput(keywordResults.getSource(), keywordResults.getVisibility(), keywordResults.getKeywords(), metadata);
    }

    public void setIdentifierMap(Map<String,String> identifierMap) {
        this.identifierMap = identifierMap;
    }

    public void setLanguagePartitioned(boolean languagePartitioned) {
        this.languagePartitioned = languagePartitioned;
    }
}
