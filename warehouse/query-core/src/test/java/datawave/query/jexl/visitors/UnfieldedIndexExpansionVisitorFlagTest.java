package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Sets;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.lookups.IndexLookup;
import datawave.query.tables.ScannerFactory;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MockMetadataHelper;
import datawave.table.constants.TableName;
import datawave.util.time.DateHelper;

/**
 * Demonstrates that {@link ShardQueryConfiguration#isUseNewIndexLookups()} does not actually gate unfielded literal (EQ/NE) expansion in
 * {@link UnfieldedIndexExpansionVisitor}.
 * <p>
 * {@code visit(ASTERNode)}/{@code visit(ASTNRNode)} explicitly branch on the flag between the new lookup ({@code createUnfieldedRegexIndexLookup}) and the
 * deprecated old path ({@code createLookup(node)}, which dispatches through {@code ShardIndexQueryTableStaticMethods.expandQueryTerms()} and ultimately the
 * {@code @Deprecated} {@code FieldNameIndexLookup}). {@code visit(ASTEQNode)}/{@code visit(ASTNENode)} do not perform the same check -- they unconditionally
 * call {@code createUnfieldedLiteralIndexLookup}, regardless of the flag's value. As a result, setting {@code useNewIndexLookups=false} to request the
 * old/deprecated behavior for a literal unfielded term has no effect: the old code path (and {@code FieldNameIndexLookup}) can never be reached for
 * {@code ==}/{@code !=} terms.
 */
public class UnfieldedIndexExpansionVisitorFlagTest {

    private static final Long TIMESTAMP = DateHelper.parse("20210101").getTime();
    private static final Value EMPTY_VALUE = new Value(new byte[0]);

    private static AccumuloClient client;

    private ShardQueryConfiguration config;
    private MockMetadataHelper metadataHelper;

    @BeforeAll
    public static void setupClass() throws Exception {
        InMemoryInstance instance = new InMemoryInstance(UnfieldedIndexExpansionVisitorFlagTest.class.toString());
        client = new InMemoryAccumuloClient("root", instance);
        client.tableOperations().create(TableName.SHARD_INDEX);

        try (BatchWriter bw = client.createBatchWriter(TableName.SHARD_INDEX,
                        new BatchWriterConfig().setMaxLatency(10, TimeUnit.SECONDS).setMaxMemory(100000L).setMaxWriteThreads(1))) {
            Mutation m = new Mutation("burrito");
            m.put(new Text("FIELD1"), new Text("20210101\0datatype"), TIMESTAMP, EMPTY_VALUE);
            bw.addMutation(m);
        }
    }

    @BeforeEach
    public void setup() {
        config = new ShardQueryConfiguration();
        config.setClient(client);
        config.setDatatypeFilter(Sets.newHashSet("datatype"));
        config.setBeginDate(new Date(0));
        config.setEndDate(new Date(System.currentTimeMillis()));

        metadataHelper = new MockMetadataHelper();
        Set<String> indexedFields = new HashSet<>();
        indexedFields.add("FIELD1");
        metadataHelper.setIndexedFields(indexedFields);
    }

    /**
     * A spy that records whether the deprecated old-lookup code path was ever invoked.
     */
    private static class SpyVisitor extends UnfieldedIndexExpansionVisitor {

        boolean oldPathInvoked = false;

        protected SpyVisitor(ShardQueryConfiguration config, ScannerFactory scannerFactory, MetadataHelper helper)
                        throws TableNotFoundException, IllegalAccessException, InstantiationException {
            super(config, scannerFactory, helper);
        }

        @Override
        protected IndexLookup createLookup(JexlNode node) {
            oldPathInvoked = true;
            return super.createLookup(node);
        }
    }

    /**
     * Sanity check: the flag DOES correctly gate the old lookup path for a regex ({@code =~}) unfielded term -- when {@code useNewIndexLookups=false},
     * {@code visit(ASTERNode)} routes through {@code createLookup(node)}.
     */
    @Test
    public void regexTermHonorsFlag() throws Exception {
        config.setUseNewIndexLookups(false);

        ScannerFactory scannerFactory = new ScannerFactory(config);
        SpyVisitor visitor = new SpyVisitor(config, scannerFactory, metadataHelper);

        ASTJexlScript script = JexlASTHelper.parseJexlQuery("_ANYFIELD_ =~ 'bur.*'");
        visitor.expand(script);

        assertTrue(visitor.oldPathInvoked, "expected the deprecated old lookup path to be used for a regex term when useNewIndexLookups=false");
    }

    /**
     * BUG: unlike the regex case above, a literal equality ({@code ==}) unfielded term never checks the flag at all, so the deprecated old lookup path (and
     * {@code FieldNameIndexLookup}) is unreachable even when the caller explicitly requests it via {@code useNewIndexLookups=false}. This assertion documents
     * the expected/correct behavior and currently fails, demonstrating the bug.
     */
    @Test
    public void literalEqualityTermIgnoresFlag() throws Exception {
        config.setUseNewIndexLookups(false);

        ScannerFactory scannerFactory = new ScannerFactory(config);
        SpyVisitor visitor = new SpyVisitor(config, scannerFactory, metadataHelper);

        ASTJexlScript script = JexlASTHelper.parseJexlQuery("_ANYFIELD_ == 'burrito'");
        visitor.expand(script);

        assertTrue(visitor.oldPathInvoked, "expected the deprecated old lookup path to be used for a literal equality term when useNewIndexLookups=false, "
                        + "but visit(ASTEQNode) always uses the new UnfieldedLiteralIndexLookup regardless of the flag");
    }
}
