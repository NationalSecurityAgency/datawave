package datawave.query.tables.keyword;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;

public class KeywordQueryTransformerTest {

    //@formatter:off
    String[] input = {
            "DOCUMENT:shard1/datatype/uid",
            "DOCUMENT:shard2/datatype/uid!PAGE_ID:12345",
            "DOCUMENT:shard3/datatype/uid!PAGE_ID:12345%LANGUAGE:ENGLISH",
            "DOCUMENT:shard4/datatype/uid%LANGUAGE:ENGLISH",

    };

    String[][] expected = {
            {"DOCUMENT", "shard1", "datatype", "uid", "", ""},
            {"DOCUMENT", "shard2", "datatype", "uid", "PAGE_ID:12345", ""},
            {"DOCUMENT", "shard3", "datatype", "uid", "PAGE_ID:12345", "ENGLISH"},
            {"DOCUMENT", "shard4", "datatype", "uid", "", "ENGLISH"},
    };

    String[] malformedInput = {
            "DOCUMENT:shard1/datatype", "shard2/datatype",
            "DOCUMENT:shard3/data!type/uid!PAGE_ID:12345%LANGUAGE:ENGLISH",
            "shard5/datatype/uid!PAGE_ID:54321",
            "shard6/datatype/uid%LANGUAGE:ENGLISH",
            "shard7/datatype/uid!PAGE_ID:54321%LANGUAGE:ENGLISH"
    };
    //@formatter:on

    @Test
    public void testWellFormedExtractIdentifiersAndLanguages() {
        // tests individual identifiers
        for (int i = 0; i < input.length; i++) {
            Query querySettings = new QueryImpl();
            querySettings.setQuery(input[i]);
            Map<String,String> identifierMap = new HashMap<>();
            Map<String,String> languageMap = new HashMap<>();
            Collection<String> queryTerms = KeywordQueryLogic.extractQueryTerms(querySettings);
            KeywordQueryLogic.extractIdentifiersAndLanguages(queryTerms, identifierMap, languageMap);

            String[] expectedResults = expected[i];

            // note: expectedResults[0] is ignored totally - it's just present for completeness.
            String key = expectedResults[1] + "/" + expectedResults[2] + "/" + expectedResults[3];

            if (expectedResults[4].isEmpty()) { // identifier
                assertNull(identifierMap.get(key));
            } else {
                assertEquals(expectedResults[4], identifierMap.get(key));
            }

            if (expectedResults[5].isEmpty()) { // language
                assertNull(languageMap.get(key));
            } else {
                assertEquals(expectedResults[5], languageMap.get(key));
            }
        }
    }

    @Test
    public void testWellFormedExtractIdentifiersAndLanguagesAggregate() {
        // tests in aggregate
        String aggregateQuery = String.join(" ", input);
        Query querySettings = new QueryImpl();
        querySettings.setQuery(aggregateQuery);
        Map<String,String> identifierMap = new HashMap<>();
        Map<String,String> languageMap = new HashMap<>();
        Collection<String> queryTerms = KeywordQueryLogic.extractQueryTerms(querySettings);
        KeywordQueryLogic.extractIdentifiersAndLanguages(queryTerms, identifierMap, languageMap);

        for (String[] expectedResults : expected) {
            String key = expectedResults[1] + "/" + expectedResults[2] + "/" + expectedResults[3];

            if (!expectedResults[4].isEmpty()) { // identifier
                assertEquals(expectedResults[4], identifierMap.remove(key));
            }

            if (!expectedResults[5].isEmpty()) { // language
                assertEquals(expectedResults[5], languageMap.remove(key));
            }
        }

        assertTrue("did not ses all expected identifiers", identifierMap.isEmpty());
        assertTrue("did not see all expected languages", languageMap.isEmpty());

    }

    @Test
    public void testMalformedIdentifiers() {
        for (String malformed : malformedInput) {
            Query querySettings = new QueryImpl();
            querySettings.setQuery(malformed);
            Map<String,String> identifierMap = new HashMap<>();
            Map<String,String> languageMap = new HashMap<>();

            try {
                Collection<String> queryTerms = KeywordQueryLogic.extractQueryTerms(querySettings);
                KeywordQueryLogic.extractIdentifiersAndLanguages(queryTerms, identifierMap, languageMap);
                fail(malformed + " did not cause an IllegalArgumentExcption");
            } catch (IllegalArgumentException e) {
                // expected
            }
        }
    }
}
