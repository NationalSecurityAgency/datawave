package datawave.query.index.day;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;

import org.apache.accumulo.core.data.Range;
import org.apache.commons.jexl3.parser.JexlNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import datawave.query.CloseableIterable;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.index.lookup.IndexInfo;
import datawave.query.index.lookup.QueryPlanStream;
import datawave.query.index.lookup.RangeStream;
import datawave.query.index.lookup.ShardSpecificIndexIterator;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.planner.QueryPlan;
import datawave.query.util.Tuple2;

/**
 * Bridges the {@link DayIndexScannerStream} with what is consumed by the ShardQueryLogic.
 * <p>
 * Primary purpose is to configure scanner streams, and add context to query plans coming off the DayIndex
 */
public class DayIndexStream implements QueryPlanStream {

    private static final Logger log = LoggerFactory.getLogger(DayIndexStream.class);

    private final DayIndexConfig config;
    private Iterator<QueryPlan> stream;

    public DayIndexStream(ShardQueryConfiguration config) {
        this.config = new DayIndexConfig(config);
    }

    @Override
    public CloseableIterable<QueryPlan> streamPlans(JexlNode node) {

        // parse query terms and invert the multimap
        Multimap<String,String> fieldsAndValues = IndexedTermVisitor.getIndexedFieldsAndValues(node, config.getIndexedFields());
        config.setValuesAndFields(Multimaps.invertFrom(fieldsAndValues, HashMultimap.create()));

        if (config.getValuesAndFields().isEmpty()) {
            log.info("query had no executable terms, falling back to full index scan list");
            stream = new FullIndexStream(config);
        } else {
            stream = new DayIndexScannerStream(config);
        }

        return this;
    }

    @Override
    public void close() throws IOException {
        if (stream instanceof Closeable) {
            ((Closeable) stream).close();
        }
    }

    @Override
    public Iterator<QueryPlan> iterator() {
        return stream;
    }

    private static class FullIndexStream implements Iterator<QueryPlan>, Closeable {

        private final DayIndexConfig config;
        private final ShardSpecificIndexIterator delegate;

        public FullIndexStream(DayIndexConfig config) {
            this.config = config;

            RangeStream.NumShardFinder numShardFinder = new RangeStream.NumShardFinder(config.getClient());
            this.delegate = new ShardSpecificIndexIterator(config.getNode(), numShardFinder, config.getStartDate(), config.getEndDate());
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public QueryPlan next() {
            Tuple2<String,IndexInfo> top = delegate.next();

            //  @formatter:off
            return new QueryPlan()
                            .withRanges(Collections.singleton(Range.exact(top.first())))
                            .withQueryTree(config.getNode())
                            .withQueryString(JexlStringBuildingVisitor.buildQuery(config.getNode()));
            //  @formatter:on
        }

        @Override
        public void close() throws IOException {
            // no-op
        }
    }
}
