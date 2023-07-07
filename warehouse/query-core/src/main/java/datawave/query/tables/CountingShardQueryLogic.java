package datawave.query.tables;

import java.io.ByteArrayOutputStream;
import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.commons.collections4.iterators.TransformIterator;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import datawave.core.iterators.ResultCountingIterator;
import datawave.marking.MarkingFunctions;
import datawave.query.DocumentSerialization;
import datawave.query.attributes.Document;
import datawave.query.config.ShardQueryConfiguration;
import datawave.query.function.deserializer.DocumentDeserializer;
import datawave.query.scheduler.PushdownScheduler;
import datawave.query.scheduler.Scheduler;
import datawave.query.tables.shard.CountAggregatingIterator;
import datawave.query.transformer.ShardQueryCountTableTransformer;
import datawave.webservice.query.Query;
import datawave.webservice.query.logic.QueryLogicTransformer;

/**
 * A simple extension of the basic ShardQueryTable which applies a counting iterator on top of the "normal" iterator stack.
 *
 *
 *
 */
public class CountingShardQueryLogic extends ShardQueryLogic {
    private static final Logger log = Logger.getLogger(CountingShardQueryLogic.class);

    public CountingShardQueryLogic() {
        super();
    }

    public CountingShardQueryLogic(CountingShardQueryLogic other) {
        super(other);
    }

    @Override
    public CountingShardQueryLogic clone() {
        return new CountingShardQueryLogic(this);
    }

    @Override
    public QueryLogicTransformer getTransformer(Query settings) {
        return new ShardQueryCountTableTransformer(settings, this.markingFunctions, this.responseObjectFactory);
    }

    @Override
    public TransformIterator getTransformIterator(Query settings) {
        return new CountAggregatingIterator(this.iterator(), getTransformer(settings));
    }

    @Override
    public Scheduler getScheduler(ShardQueryConfiguration config, ScannerFactory scannerFactory) {
        PushdownScheduler scheduler = new PushdownScheduler(config, scannerFactory, this.metadataHelperFactory);
        if (!config.getUsePartialInterpreter()) {
            scheduler.addSetting(new IteratorSetting(config.getBaseIteratorPriority() + 50, "counter", ResultCountingIterator.class.getName()));
        }
        return scheduler;
    }

    @Override
    public Iterator iterator() {
        if (getConfig().getUsePartialInterpreter()) {
            return new WebserviceResultCountingIterator(iterator, getConfig().getReturnType());
        } else {
            return iterator;
        }
    }

    /**
     * Webservice side implementation of the {@link ResultCountingIterator}
     */
    private class WebserviceResultCountingIterator implements Iterator<Map.Entry<Key,Value>> {

        private long count = 0;
        private Map.Entry<Key,Value> tk;
        private Iterator<Map.Entry<Key,Value>> iter;
        private DocumentDeserializer de;
        private MarkingFunctions markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();

        public WebserviceResultCountingIterator(Iterator<Map.Entry<Key,Value>> iter, DocumentSerialization.ReturnType returnType) {
            this.iter = iter;
            this.de = DocumentSerialization.getDocumentDeserializer(returnType);
        }

        @Override
        public boolean hasNext() {
            return iter.hasNext();
        }

        @Override
        public Map.Entry<Key,Value> next() {
            consume();
            return tk;
        }

        /**
         * See {@link ResultCountingIterator#consume()}
         */
        private void consume() {
            ColumnVisibility cv = new ColumnVisibility();

            while (iter.hasNext()) {
                tk = iter.next();
                cv = getAndCombineCV(tk.getValue(), cv);
                count++;
            }

            tk = new AbstractMap.SimpleEntry<>(tk.getKey(), createResultValue(this.count, cv));
        }

        private ColumnVisibility getAndCombineCV(Value value, ColumnVisibility cv) {
            Set<ColumnVisibility> cvs = new HashSet<>();
            Map.Entry<Key,Document> deserialized = de.apply(new AbstractMap.SimpleEntry<>(new Key(), value));

            cvs.add(cv); // add original rolled up cv
            cvs.add(deserialized.getValue().getColumnVisibility());

            try {
                return markingFunctions.combine(cvs);
            } catch (MarkingFunctions.Exception e) {
                e.printStackTrace();
                return cv;
            }
        }

        private Value createResultValue(long count, ColumnVisibility cv) {
            ResultCountingIterator.ResultCountTuple result = new ResultCountingIterator.ResultCountTuple(count, cv);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            Output kryoOutput = new Output(baos);
            new Kryo().writeObject(kryoOutput, result);
            kryoOutput.close();

            return new Value(baos.toByteArray());
        }
    }

}
