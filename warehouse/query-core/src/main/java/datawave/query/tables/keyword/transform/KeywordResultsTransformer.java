package datawave.query.tables.keyword.transform;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import datawave.util.keyword.KeywordResults;
import datawave.util.keyword.TagCloudPartition;

public class KeywordResultsTransformer implements TagCloudInputTransformer<KeywordResults> {
    private static final String LABEL = "keywords";

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

        String partition = "";
        if (languagePartitioned && keywordResults.getMetadata().get("language") != null) {
            partition = keywordResults.getMetadata().get("language");
        }

        return new TagCloudPartition(partition, LABEL, List.of(keywordResults));
    }

    public void setIdentifierMap(Map<String,String> identifierMap) {
        this.identifierMap = identifierMap;
    }

    public void setLanguagePartitioned(boolean languagePartitioned) {
        this.languagePartitioned = languagePartitioned;
    }
}
