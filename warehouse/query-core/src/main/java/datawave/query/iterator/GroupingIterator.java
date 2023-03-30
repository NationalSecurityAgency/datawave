package datawave.query.iterator;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import datawave.data.type.NumberType;
import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.common.grouping.GroupingUtil;
import datawave.query.common.grouping.GroupingUtil.GroupCountingHashMap;
import datawave.query.common.grouping.GroupingUtil.GroupingTypeAttribute;
import datawave.query.jexl.JexlASTHelper;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;
import org.springframework.util.Assert;

import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * Because the t-server may tear down and start a new iterator at any time after a next() call, there can be no saved state in this class. For that reason, each
 * next call on the t-server will flatten the aggregated data into a single Entry&gt;Key,Document&lt; to return to the web server.
 */
public class GroupingIterator implements Iterator<Map.Entry<Key,Document>> {
    
    private static final Logger log = getLogger(GroupingIterator.class);
    
    /**
     * the fields (user provided) to group by
     */
    private final Set<String> groupFieldsSet;
    
    /**
     * A map of TypeAttribute collection keys to integer counts This map uses a special key type that ignores the metadata (with visibilities) in its hashCode
     * and equals methods
     */
    private GroupCountingHashMap countingMap;
    
    /**
     * holds the aggregated column visibilities for each grouped event
     */
    private Multimap<Collection<GroupingTypeAttribute<?>>,ColumnVisibility> fieldVisibilities = HashMultimap.create();
    
    private final GroupingUtil groupingUtil = new GroupingUtil();
    
    /**
     * list of keys that have been read, in order to keep track of where we left off when a new iterator is created
     */
    private final List<Key> keys = new ArrayList<>();
    
    private final MarkingFunctions markingFunctions;
    
    private final int groupFieldsBatchSize;
    
    private final YieldCallback<Key> yieldCallback;
    
    private final Iterator<Map.Entry<Key,Document>> previousIterators;
    
    private final LinkedList<Document> documents = new LinkedList<>();
    
    Map.Entry<Key,Document> next;
    
    /**
     * Length of time in milliseconds that a client will wait for a results to be returned. If a result is not collected before the timeout, a key with an
     * "intermediate" document will be returned.
     */
    private final long resultTimeout;
    
    public GroupingIterator(Iterator<Map.Entry<Key,Document>> previousIterators, MarkingFunctions markingFunctions, Collection<String> groupFieldsSet,
                    int groupFieldsBatchSize, YieldCallback<Key> yieldCallback, long resultTimeout) {
        this.previousIterators = previousIterators;
        this.markingFunctions = markingFunctions;
        this.groupFieldsSet = groupFieldsSet.stream().map(JexlASTHelper::deconstructIdentifier).collect(Collectors.toSet());
        this.groupFieldsBatchSize = groupFieldsBatchSize;
        this.yieldCallback = yieldCallback;
        
        this.countingMap = new GroupCountingHashMap(this.markingFunctions);
        
        this.resultTimeout = resultTimeout;
    }
    
    @Override
    public boolean hasNext() {
        for (int i = 0; i < groupFieldsBatchSize; i++) {
            if (previousIterators.hasNext()) {
                Map.Entry<Key,Document> entry = previousIterators.next();
                if (entry != null) {
                    log.trace("t-server get list key counts for: {}", entry);
                    
                    keys.add(entry.getKey());
                    GroupingUtil.GroupingInfo groupingInfo = groupingUtil.getGroupingInfo(entry, groupFieldsSet, this.countingMap);
                    this.countingMap = groupingInfo.getCountsMap();
                    this.fieldVisibilities = groupingInfo.getFieldVisibilities();
                }
            } else if (yieldCallback != null && yieldCallback.hasYielded()) {
                log.trace("hasNext is false because yield was called");
                if (countingMap != null && !countingMap.isEmpty()) {
                    // reset the yield and use its key in the flattened document prepared below
                    keys.add(yieldCallback.getPositionAndReset());
                }
                break;
            } else {
                // in.hasNext() was false and there was no yield
                break;
            }
        }
        
        Document document = null;
        next = null;
        
        if (countingMap != null && !countingMap.isEmpty()) {
            
            log.trace("hasNext() will use the countingMap: {}", countingMap);
            
            for (Collection<GroupingTypeAttribute<?>> entry : countingMap.keySet()) {
                log.trace("from countingMap, got entry: {}", entry);
                ColumnVisibility columnVisibility;
                try {
                    columnVisibility = groupingUtil.combine(fieldVisibilities.get(entry), markingFunctions);
                } catch (Exception e) {
                    throw new IllegalStateException("Unable to merge column visibilities: " + fieldVisibilities.get(entry), e);
                }
                // grab a key from those saved during getListKeyCounts
                Assert.notEmpty(keys, "no available keys for grouping results");
                // use the last (most recent) key so a new iterator will know where to start
                Key docKey = keys.get(keys.size() - 1);
                Document d = new Document(docKey, true);
                d.setColumnVisibility(columnVisibility);
                
                entry.forEach(base -> d.put(base.getMetadata().getRow().toString(), base));
                NumberType type = new NumberType();
                type.setDelegate(new BigDecimal(countingMap.get(entry)));
                TypeAttribute<BigDecimal> attr = new TypeAttribute<>(type, new Key("count"), true);
                d.put("COUNT", attr);
                documents.add(d);
            }
            // flatten to just one document
            document = flatten(documents);
        }
        
        if (document != null) {
            Key key;
            if (keys.size() > 0) {
                // use the last (most recent) key so a new iterator will know where to start
                key = keys.get(keys.size() - 1);
            } else {
                key = document.getMetadata();
            }
            next = Maps.immutableEntry(key, document);
            log.trace("hasNext {}", next);
            countingMap.clear();
            return true;
        }
        
        return false;
    }
    
    @Override
    public Map.Entry<Key,Document> next() {
        return new AbstractMap.SimpleEntry<>(next.getKey(), next.getValue());
    }
    
    /**
     * <pre>
     * flush used the countingMap:
     * [[MALE, 16],
     * [MALE, 20],
     * [40, MALE],
     * [40, MALE],
     * [MALE, 22] x 2,
     * [FEMALE, 18],
     * [MALE, 24],
     * [20, MALE],
     * [30, MALE],
     * [FEMALE, 18],
     * [34, MALE]]
     * 
     * to create documents list: [
     * {AGE=16, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=1, ETA=20, GENERE=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=1, ETA=40, GENERE=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=40, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=2, ETA=22, GENERE=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=18, COUNT=1, GENDER=FEMALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=1, ETA=24, GENERE=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=20, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=30, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {COUNT=1, ETA=18, GENERE=FEMALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false,
     * {AGE=34, COUNT=1, GENDER=MALE}:20130101_0 test%00;-d5uxna.msizfm.-oxy0iu: [ALL] 1356998400000 false]
     * 
     * which is then flattened to just one document with the fields and counts correlated with a grouping context suffix:
     * 
     * {
     * AGE.0=16, GENDER.0=MALE, COUNT.0=1,
     * ETA.1=20, GENERE.1=MALE, COUNT.1=1,
     * ETA.2=40, GENERE.2=MALE, COUNT.2=1,
     * AGE.3=40, GENDER.3=MALE, COUNT.3=1,
     * ETA.4=22, GENERE.4=MALE, COUNT.4=2,
     * AGE.5=18, GENDER.5=FEMALE, COUNT.5=1,
     * ETA.6=24, GENERE.6=MALE, COUNT.6=1,
     * AGE.7=20, GENDER.7=MALE, COUNT.7=1,
     * AGE.8=30, GENDER.8=MALE, COUNT.8=1,
     * ETA.9=18, GENERE.9=FEMALE, COUNT.9=1,
     * AGE.A=34, GENDER.A=MALE, COUNT.A=1,
     * }
     * </pre>
     *
     * The Attributes, which have had their visibilities merged, are copied into normal TypeAttributes for serialization to the webserver.
     *
     * @param documents
     *            the list of documents to flatten into a single document
     * @return a flattened document
     */
    private Document flatten(List<Document> documents) {
        log.trace("flatten {}", documents);
        Document theDocument = new Document(documents.get(documents.size() - 1).getMetadata(), true);
        int context = 0;
        Set<ColumnVisibility> visibilities = new HashSet<>();
        for (Document document : documents) {
            log.trace("document: {}", document);
            for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : document.entrySet()) {
                String name = entry.getKey();
                visibilities.add(entry.getValue().getColumnVisibility());
                
                Attribute<? extends Comparable<?>> attribute = entry.getValue();
                attribute.setColumnVisibility(entry.getValue().getColumnVisibility());
                // call copy() on the GroupingTypeAttribute to get a plain TypeAttribute
                // instead of a GroupingTypeAttribute that is package protected and won't serialize
                theDocument.put(name + "." + Integer.toHexString(context).toUpperCase(), (TypeAttribute) attribute.copy(), true, false);
            }
            context++;
        }
        ColumnVisibility combinedVisibility = groupingUtil.combine(visibilities, markingFunctions);
        log.trace("combined visibilities: {} to {}", visibilities, combinedVisibility);
        theDocument.setColumnVisibility(combinedVisibility);
        // documents.clear();
        log.trace("flattened document: {}", theDocument);
        return theDocument;
    }
    
}
