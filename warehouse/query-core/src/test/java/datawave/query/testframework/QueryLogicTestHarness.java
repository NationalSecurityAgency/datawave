package datawave.query.testframework;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.Set;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.log4j.Logger;
import org.junit.Assert;

import datawave.core.query.configuration.CheckpointableQueryConfiguration;
import datawave.core.query.configuration.GenericQueryConfiguration;
import datawave.core.query.logic.BaseQueryLogic;
import datawave.core.query.logic.CheckpointableQueryLogic;
import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.core.query.logic.QueryLogicFactory;
import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.TimingMetadata;
import datawave.query.attributes.TypeAttribute;
import datawave.query.function.deserializer.KryoDocumentDeserializer;
import datawave.query.iterator.profile.FinalDocumentTrackingIterator;
import datawave.webservice.query.exception.QueryException;

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

    private void dumpCp(String start, QueryCheckpoint cp) {
        cp.getQueries().iterator().forEachRemaining(qd -> {
            System.out.println(">>>> " + start + ": " + qd.getRanges() + " -> " + qd.getLastResult());
        });
    }

    /**
     * Determines if the correct results were obtained for a query.
     *
     * @param logic
     *            key/value response data
     * @param factory
     *            a logic factory for teardown/rebuilds if the logic is checkpointable
     * @param expected
     *            list of key values expected within response data
     * @param checkers
     *            list of additional validation methods
     */
    public void assertLogicResults(BaseQueryLogic<Map.Entry<Key,Value>> logic, QueryLogicFactory factory, Collection<String> expected,
                    List<DocumentChecker> checkers) throws IOException, ClassNotFoundException {
        Set<String> actualResults = new HashSet<>();

        if (log.isDebugEnabled()) {
            log.debug("    ======  expected id(s)  ======");
            for (String e : expected) {
                log.debug("id(" + e + ")");
            }
        }

        boolean disableCheckpoint = false;
        if (!disableCheckpoint && logic instanceof CheckpointableQueryLogic && ((CheckpointableQueryLogic) logic).isCheckpointable() && factory != null) {
            Queue<QueryCheckpoint> cps = new LinkedList<>();
            GenericQueryConfiguration config = logic.getConfig();
            AccumuloClient client = config.getClient();
            QueryKey queryKey = new QueryKey("default", logic.getConfig().getQuery().getId().toString(), logic.getLogicName());
            // replace the config with that which would have been stored
            if (config instanceof CheckpointableQueryConfiguration && ((CheckpointableQueryLogic) logic).isCheckpointable()) {
                config = ((CheckpointableQueryConfiguration) config).checkpoint();
            }

            cps.addAll(((CheckpointableQueryLogic) logic).checkpoint(queryKey));
            while (!cps.isEmpty()) {
                QueryCheckpoint cp = cps.remove();
                // create a new instance of the logic
                try {
                    logic = (BaseQueryLogic<Map.Entry<Key,Value>>) factory.getQueryLogic(logic.getLogicName());
                } catch (CloneNotSupportedException | QueryException e) {
                    Assert.fail("Failed to recreate checkpointable query logic  for " + logic.getLogicName() + ": " + e.getMessage());
                }
                // now reset the logic given the checkpoint
                try {
                    ((CheckpointableQueryLogic) logic).setupQuery(client, config, cp);
                } catch (Exception e) {
                    log.error("Failed to setup query given last checkpoint", e);
                    Assert.fail("Failed to setup query given last checkpoint: " + e.getMessage());
                }
                Iterator<Map.Entry<Key,Value>> iter = logic.iterator();
                if (iter.hasNext()) {
                    Map.Entry<Key,Value> next = iter.next();
                    actualResults = processResult(actualResults, next, checkers);
                    cps.addAll(((CheckpointableQueryLogic) logic).checkpoint(queryKey));
                }
            }
        } else {
            for (Map.Entry<Key,Value> entry : logic) {
                actualResults = processResult(actualResults, entry, checkers);
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

    /**
     * Given an entry off of the logic iterator, deserialize and check its validity and add to the actualResults
     *
     * @param actualResults
     * @param entry
     * @param checkers
     */
    private Set<String> processResult(Set<String> actualResults, Map.Entry<Key,Value> entry, List<DocumentChecker> checkers) {
        if (FinalDocumentTrackingIterator.isFinalDocumentKey(entry.getKey())) {
            return actualResults;
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
            return actualResults;
        }

        // parse the document
        String extractedResult = this.parser.parse(entry.getKey(), document);
        log.debug("result(" + extractedResult + ") key(" + entry.getKey() + ") document(" + document + ")");

        // verify expected results
        Assert.assertNotNull("extracted result", extractedResult);
        Assert.assertFalse("duplicate result(" + extractedResult + ") key(" + entry.getKey() + ")", actualResults.contains(extractedResult));

        // perform any custom assert checks on document
        for (final DocumentChecker check : checkers) {
            check.assertValid(document);
        }

        actualResults.add(extractedResult);
        return actualResults;
    }

}
