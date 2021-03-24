package datawave.query.testframework;

import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.TimingMetadata;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.webservice.query.logic.BaseQueryLogic;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class QueryLogicTestHarness {
    
    private static final Logger log = Logger.getLogger(QueryLogicTestHarness.class);
    
    private final TestResultParser parser;
    private final KryoDocumentDeserializer deserializer;
    
    public QueryLogicTestHarness(final TestResultParser testParser) {
        this.parser = testParser;
        this.deserializer = new KryoDocumentDeserializer();
    }
    
    // =============================================
    // interfaces for lambda functions
    
    /**
     * Assert checking for any specific conditions.
     */
    public interface DocumentChecker {
        /**
         * Assert any special conditions that are expected for an entry.
         *
         * @param doc
         *            deserialized entry
         */
        void assertValid(Document doc);
    }
    
    public interface TestResultParser {
        String parse(Key key, Document document);
    }
    
    // =============================================
    // assert methods
    
    /**
     * Determines if the correct results were obtained for a query.
     * 
     * @param logic
     *            key/value response data
     * @param expected
     *            list of key values expected within response data
     * @param checkers
     *            list of additional validation methods
     */
    public void assertLogicResults(BaseQueryLogic<Map.Entry<Key,Value>> logic, Collection<String> expected, List<DocumentChecker> checkers) {
        Set<String> actualResults = new HashSet<>();
        
        if (log.isDebugEnabled()) {
            log.debug("    ======  expected id(s)  ======");
            for (String e : expected) {
                log.debug("id(" + e + ")");
            }
        }
        
        for (Map.Entry<Key,Value> entry : logic) {
            if (FinalDocumentTrackingIterator.isFinalDocumentKey(entry.getKey())) {
                continue;
            }
            
            final Document document = this.deserializer.apply(entry).getValue();
            
            // check all of the types to ensure that all are keepers as defined in the
            // AttributeFactory class
            int count = 0;
            for (Attribute<? extends Comparable<?>> attribute : document.getAttributes()) {
                if (attribute instanceof TimingMetadata) {
                    // ignore
                } else if (attribute instanceof Attributes) {
                    Attributes attrs = (Attributes) attribute;
                    Collection<Class<?>> types = new HashSet<>();
                    for (Attribute<? extends Comparable<?>> attr : attrs.getAttributes()) {
                        count++;
                        if (attr instanceof TypeAttribute) {
                            Type<? extends Comparable<?>> type = ((TypeAttribute<?>) attr).getType();
                            if (Objects.nonNull(type)) {
                                types.add(type.getClass());
                            }
                        }
                    }
                    Assert.assertEquals(AttributeFactory.getKeepers(types), types);
                } else {
                    count++;
                }
            }
            
            // ignore empty documents (possible when only passing FinalDocument back)
            if (count == 0) {
                continue;
            }
            
            // parse the document
            String extractedResult = this.parser.parse(entry.getKey(), document);
            log.debug("result(" + extractedResult + ") key(" + entry.getKey() + ") document(" + document + ")");
            
            // verify expected results
            Assert.assertNotNull("extracted result", extractedResult);
            Assert.assertFalse("duplicate result(" + extractedResult + ") key(" + entry.getKey() + ")", actualResults.contains(extractedResult));
            actualResults.add(extractedResult);
            
            // perform any custom assert checks on document
            for (final DocumentChecker check : checkers) {
                check.assertValid(document);
            }
        }
        
        log.info("total records found(" + actualResults.size() + ") expected(" + expected.size() + ")");
        
        // ensure that the complete expected result set exists
        if (expected.size() > actualResults.size()) {
            final Set<String> notFound = new HashSet<>(expected);
            notFound.removeAll(actualResults);
            for (final String m : notFound) {
                log.error("missing result(" + m + ")");
            }
        } else if (expected.size() < actualResults.size()) {
            final Set<String> extra = new HashSet<>(actualResults);
            extra.removeAll(expected);
            for (final String r : extra) {
                log.error("unexpected result(" + r + ")");
            }
        }
        
        Assert.assertEquals("results do not match expected", expected.size(), actualResults.size());
        Assert.assertTrue("expected and actual values do not match", expected.containsAll(actualResults));
        Assert.assertTrue("expected and actual values do not match", actualResults.containsAll(expected));
    }
}
