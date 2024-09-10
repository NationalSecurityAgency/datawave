package datawave.query.index.lookup;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTERNode;
import org.apache.commons.jexl3.parser.ASTFunctionNode;
import org.apache.commons.jexl3.parser.ASTGENode;
import org.apache.commons.jexl3.parser.ASTGTNode;
import org.apache.commons.jexl3.parser.ASTLENode;
import org.apache.commons.jexl3.parser.ASTLTNode;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.exceptions.DatawaveFatalQueryException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.JexlNodeFactory;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.tables.RangeStreamScanner;
import datawave.query.tables.ScannerFactory;
import datawave.query.tables.SessionOptions;
import datawave.query.util.MetadataHelper;
import datawave.query.util.QueryScannerHelper;
import datawave.util.time.DateHelper;

/**
 * A RangeStream optimized for fast lookups of low cardinality terms (lookup uuid).
 */
public class FindFirstRangeStream extends RangeStream {

    private static final Logger log = LoggerFactory.getLogger(FindFirstRangeStream.class);

    public FindFirstRangeStream(ShardQueryConfiguration config, ScannerFactory scanners, MetadataHelper metadataHelper) {
        super(config, scanners, metadataHelper);
    }

    @Override
    public IndexStream visit(ASTOrNode node, Object data) {
        Multimap<String,String> valueFieldMap = collectFieldValuesForUnion(node);
        List<ConcurrentScannerInitializer> tasks = collectInitializationTasks(valueFieldMap);

        Union.Builder builder = Union.builder();
        builder.addChildren(tasks);
        Union union = builder.build(executor);

        switch (union.context()) {
            case PRESENT:
            case VARIABLE:
                return union;
            default:
                // we don't care about returning a special context to the query planner -- either the data is present or it isn't
                return ScannerStream.noData(union.currentNode(), union);
        }
    }

    /**
     * Traverse the union and collect all field values, placing them into an inverted value-field multimap
     *
     * @param node
     *            the union
     * @return the multimap of values and fields
     */
    private Multimap<String,String> collectFieldValuesForUnion(ASTOrNode node) {
        Multimap<String,String> valueFieldMap = HashMultimap.create();

        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = JexlASTHelper.dereference(node.jjtGetChild(i));

            if (child instanceof ASTEQNode) {
                String field = JexlASTHelper.getIdentifier(node);
                String value = (String) JexlASTHelper.getLiteralValueSafely(node);

                if (value == null) {
                    throw new IllegalStateException("FindFirst does not support null literals");
                }

                valueFieldMap.put(value, field);

            } else {
                throw new IllegalStateException("FindFirst does not support non-equality terms");
            }
        }

        return valueFieldMap;
    }

    private List<ConcurrentScannerInitializer> collectInitializationTasks(Multimap<String,String> valueFieldMap) {
        List<ConcurrentScannerInitializer> tasks = new ArrayList<>();

        for (String value : new TreeSet<>(valueFieldMap.keySet())) {
            Collection<String> fields = valueFieldMap.get(value);

            ScannerStream stream = buildFindFirstScanner(value, fields);
            tasks.add(new ConcurrentScannerInitializer(stream));
        }

        return tasks;
    }

    private ScannerStream buildFindFirstScanner(String value, Collection<String> fields) {
        try {
            SessionOptions options = new SessionOptions();

            for (String field : fields) {
                options.fetchColumnFamily(new Text(field));
            }

            int priority = config.getBaseIteratorPriority();

            if (!config.getDatatypeFilter().isEmpty()) {
                options.addScanIterator(makeDataTypeFilter(config, ++priority));
            }

            if (!createUidsIteratorClass.isAssignableFrom(FindFirstUidIterator.class)) {
                log.warn("Ignoring iterator class: {}", createUidsIteratorClass.getSimpleName());
            }

            IteratorSetting settings = new IteratorSetting(++priority, FindFirstUidIterator.class);
            settings.addOption(FindFirstUidIterator.FIELDS_OPT, Joiner.on(',').join(fields));
            if (config.getCollapseUids()) {
                settings.addOption(FindFirstUidIterator.COLLAPSE_OPT, Boolean.toString(true));
            }
            if (config.getParseTldUids()) {
                settings.addOption(FindFirstUidIterator.IS_TLD_OPT, Boolean.toString(true));
            }
            options.addScanIterator(settings);

            // for single term queries this is the entire query
            // for multi-value queries this is a sub-query
            ASTOrNode node = buildNode(value, fields);
            options.addScanIterator(QueryScannerHelper.getQueryInfoIterator(config.getQuery(), false, JexlStringBuildingVisitor.buildQuery(node)));

            RangeStreamScanner scanner = scanners.newRangeScanner(config.getIndexTableName(), config.getAuthorizations(), config.getQuery());
            scanner.setOptions(options);
            scanner.setMaxResults(config.getMaxIndexBatchSize());
            scanner.setExecutor(streamExecutor);

            Range range = buildRange(value, fields);
            scanner.setRanges(Collections.singleton(range));

            // need to build a special entry parser since we're pushing multiple fields into the same scanner
            EntryParser parser = new FindFirstEntryParser(node);

            return ScannerStream.initialized(scanner, parser, node);
        } catch (Exception e) {
            log.error("Error building scanner", e);
            throw new DatawaveFatalQueryException("Error building scanner");
        }
    }

    private Range buildRange(String value, Collection<String> fields) {
        TreeSet<String> sorted = new TreeSet<>(fields);
        Key start = new Key(value, sorted.first(), DateHelper.format(config.getBeginDate()));
        Key stop = new Key(value, sorted.last(), DateHelper.format(config.getEndDate()) + "_" + '\uffff');
        return new Range(start, true, stop, false);
    }

    private ASTOrNode buildNode(String value, Collection<String> fields) {
        List<JexlNode> children = new LinkedList<>();
        for (String field : fields) {
            children.add(JexlNodeFactory.buildEQNode(field, value));
        }
        return (ASTOrNode) JexlNodeFactory.createOrNode(children);
    }

    @Override
    public ScannerStream visit(ASTEQNode node, Object data) {
        String field = JexlASTHelper.getIdentifier(node);
        String value = (String) JexlASTHelper.getLiteralValueSafely(node);

        if (value == null) {
            return ScannerStream.noData(node);
        }

        return buildFindFirstScanner(value, Set.of(field));
    }

    @Override
    public IndexStream visit(ASTAndNode node, Object data) {
        throw new IllegalStateException("FindFirst does not support intersections");
    }

    @Override
    public IndexStream visit(ASTFunctionNode node, Object data) {
        throw new IllegalStateException("FindFirst does not support functions");
    }

    @Override
    public IndexStream visit(ASTNENode node, Object data) {
        throw new IllegalStateException("FindFirst does not support NE nodes");
    }

    @Override
    public IndexStream visit(ASTNotNode node, Object data) {
        throw new IllegalStateException("FindFirst does not support Not nodes");
    }

    @Override
    public IndexStream visit(ASTLTNode node, Object data) {
        throw new IllegalStateException("FindFirst does not support LT nodes");
    }

    @Override
    public IndexStream visit(ASTLENode node, Object data) {
        throw new IllegalStateException("FindFirst does not support LE nodes");
    }

    @Override
    public IndexStream visit(ASTGTNode node, Object data) {
        throw new IllegalStateException("FindFirst does not support GT nodes");
    }

    @Override
    public IndexStream visit(ASTGENode node, Object data) {
        throw new IllegalStateException("FindFirst does not support GE nodes");
    }

    @Override
    public IndexStream visit(ASTERNode node, Object data) {
        throw new IllegalStateException("FindFirst does not support ER nodes");
    }

    @Override
    public IndexStream visit(ASTNRNode node, Object data) {
        throw new IllegalStateException("FindFirst does not support NR nodes");
    }
}
