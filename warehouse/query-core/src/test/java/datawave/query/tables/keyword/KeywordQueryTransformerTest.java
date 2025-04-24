package datawave.query.tables.keyword;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.transformer.KeywordQueryTransformer;
import datawave.webservice.query.result.event.Metadata;

public class KeywordQueryTransformerTest {

    String[] input = {"DOCUMENT:shard1/datatype/uid", "DOCUMENT:shard2/datatype/uid!PAGE_ID:12345",
            "DOCUMENT:shard3/datatype/uid!PAGE_ID:12345%LANGUAGE:ENGLISH", "DOCUMENT:shard4/datatype/uid%LANGUAGE:ENGLISH",

    };

    String[][] expected = {{"DOCUMENT", "shard1", "datatype", "uid", "", ""}, {"DOCUMENT", "shard2", "datatype", "uid", "PAGE_ID:12345", ""},
            {"DOCUMENT", "shard3", "datatype", "uid", "PAGE_ID:12345", "ENGLISH"}, {"DOCUMENT", "shard4", "datatype", "uid", "", "ENGLISH"},};

    String[] malformedInput = {"DOCUMENT:shard1/datatype", "shard2/datatype", "DOCUMENT:shard3/data!type/uid!PAGE_ID:12345%LANGUAGE:ENGLISH",
            "shard5/datatype/uid!PAGE_ID:54321", "shard6/datatype/uid%LANGUAGE:ENGLISH", "shard7/datatype/uid!PAGE_ID:54321%LANGUAGE:ENGLISH"};

    @Test
    public void testWellFormedExtractIdentifiersAndLanguages() {
        // tests individual identifiers
        for (int i = 0; i < input.length; i++) {
            Query querySettings = new QueryImpl();
            querySettings.setQuery(input[i]);
            Map<Metadata,String> identifierMap = new HashMap<>();
            Map<Metadata,String> languageMap = new HashMap<>();
            KeywordQueryTransformer.extractIdentifiersAndLanguages(querySettings, identifierMap, languageMap);

            String[] expectedResults = expected[i];

            // note: expectedResults[0] is ignored totally - it's just present for completeness.

            Metadata md = new Metadata();
            md.setRow(expectedResults[1]);
            md.setDataType(expectedResults[2]);
            md.setInternalId(expectedResults[3]);

            if (expectedResults[4].isEmpty()) { // identifier
                assertNull(identifierMap.get(md));
            } else {
                assertEquals(expectedResults[4], identifierMap.get(md));
            }

            if (expectedResults[5].isEmpty()) { // language
                assertNull(languageMap.get(md));
            } else {
                assertEquals(expectedResults[5], languageMap.get(md));
            }
        }
    }

    @Test
    public void testWellFormedExtractIdentifiersAndLanguagesAggregate() {
        // tests in aggregate
        String aggregateQuery = String.join(" ", input);
        Query querySettings = new QueryImpl();
        querySettings.setQuery(aggregateQuery);
        Map<Metadata,String> identifierMap = new HashMap<>();
        Map<Metadata,String> languageMap = new HashMap<>();
        KeywordQueryTransformer.extractIdentifiersAndLanguages(querySettings, identifierMap, languageMap);

        for (String[] expectedResults : expected) {
            Metadata md = new Metadata();
            md.setRow(expectedResults[1]);
            md.setDataType(expectedResults[2]);
            md.setInternalId(expectedResults[3]);

            if (!expectedResults[4].isEmpty()) { // identifier
                assertEquals(expectedResults[4], identifierMap.remove(md));
            }

            if (!expectedResults[5].isEmpty()) { // language
                assertEquals(expectedResults[5], languageMap.remove(md));
            }
        }

        assertTrue("did not ses all expected identifiers", identifierMap.isEmpty());
        assertTrue("did not see all expected languages", languageMap.isEmpty());

    }

    @Test
    public void testMaformedIdentifiers() {
        for (String malformed : malformedInput) {
            Query querySettings = new QueryImpl();
            querySettings.setQuery(malformed);
            Map<Metadata,String> identifierMap = new HashMap<>();
            Map<Metadata,String> languageMap = new HashMap<>();

            try {
                KeywordQueryTransformer.extractIdentifiersAndLanguages(querySettings, identifierMap, languageMap);
                fail(malformed + " did not cause an IllegalArgumentExcption");
            } catch (IllegalArgumentException e) {
                // expected
            }
        }
    }
}
