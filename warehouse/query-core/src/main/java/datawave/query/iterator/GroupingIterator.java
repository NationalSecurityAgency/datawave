package datawave.query.iterator;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.iterators.YieldCallback;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.slf4j.Logger;

import com.google.common.collect.Maps;

import datawave.marking.MarkingFunctions;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.query.common.grouping.DocumentGrouper;
import datawave.query.common.grouping.Group;
import datawave.query.common.grouping.GroupFields;
import datawave.query.common.grouping.GroupingUtils;
import datawave.query.common.grouping.Groups;

/**
 * Because the t-server may tear down and start a new iterator at any time after a next() call, there can be no saved state in this class. For that reason, each
 * next call on the t-server will flatten the aggregated data into a single Entry&gt;Key,Document&lt; to return to the web server.
 */
public class GroupingIterator implements Iterator<Map.Entry<Key,Document>> {

    private static final Logger log = getLogger(GroupingIterator.class);

    /**
     * The fields to group and aggregate by.
     */
    private final GroupFields groupFields;

    /**
     * The groups. This is updated each time
     */
    private final Groups groups;

    // the most recent key seen, used to track where we left off
    private Key mostRecentKey = null;

    // track the number of documents seen by this iterator
    private long documentCount = 0L;

    private final MarkingFunctions markingFunctions;

    private final int groupFieldsBatchSize;

    private final YieldCallback<Key> yieldCallback;

    private final Iterator<Map.Entry<Key,Document>> previousIterators;

    Map.Entry<Key,Document> next;

    public GroupingIterator(Iterator<Map.Entry<Key,Document>> previousIterators, MarkingFunctions markingFunctions, GroupFields groupFields,
                    int groupFieldsBatchSize, YieldCallback<Key> yieldCallback) {
        this.previousIterators = previousIterators;
        this.markingFunctions = markingFunctions;
        this.groupFields = groupFields;
        this.groupFieldsBatchSize = groupFieldsBatchSize;
        this.yieldCallback = yieldCallback;
        this.groups = new Groups();
    }

    @Override
    public boolean hasNext() {
        for (int i = 0; i < groupFieldsBatchSize; i++) {
            if (previousIterators.hasNext()) {
                Map.Entry<Key,Document> entry = previousIterators.next();
                if (entry != null) {
                    log.trace("t-server get list key counts for: {}", entry);
                    documentCount++;
                    mostRecentKey = entry.getKey();
                    DocumentGrouper.group(entry, groupFields, groups);
                }
            } else if (yieldCallback != null && yieldCallback.hasYielded()) {
                log.trace("hasNext is false because yield was called");
                if (!groups.isEmpty()) {
                    // reset the yield and use its key in the flattened document prepared below
                    mostRecentKey = yieldCallback.getPositionAndReset();
                }
                break;
            } else {
                // in.hasNext() was false and there was no yield
                log.trace("GroupingIterator saw {} documents producing {} groups", documentCount, groups.getGroups().size());
                break;
            }
        }

        LinkedList<Document> documents = new LinkedList<>();
        Document document = null;
        next = null;

        if (!groups.isEmpty()) {
            for (Group group : groups.getGroups()) {
                documents.add(GroupingUtils.createDocument(group, mostRecentKey, markingFunctions,
                                GroupingUtils.AverageAggregatorWriteFormat.NUMERATOR_AND_DIVISOR));
            }
            document = flatten(documents);
        }

        if (document != null) {
            Key key;
            if (mostRecentKey != null) {
                // use the last (most recent) key so a new iterator will know where to start
                key = mostRecentKey;
            } else {
                key = document.getMetadata();
            }
            next = Maps.immutableEntry(key, document);
            log.trace("hasNext {}", next);
            groups.clear();
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
        log.trace("Flattening {}", documents);

        Document flattened = new Document(documents.get(documents.size() - 1).getMetadata(), true);

        int context = 0;
        Set<ColumnVisibility> visibilities = new HashSet<>();
        for (Document document : documents) {
            log.trace("document: {}", document);
            for (Map.Entry<String,Attribute<? extends Comparable<?>>> entry : document.entrySet()) {
                visibilities.add(entry.getValue().getColumnVisibility());
                // Add a copy of each attribute to the flattened document with the context appended to the key, e.g. AGE becomes AGE.0.
                Attribute<? extends Comparable<?>> attribute = entry.getValue();
                attribute.setColumnVisibility(entry.getValue().getColumnVisibility());
                // Call copy() on the GroupingTypeAttribute to get a plain TypeAttribute instead of a GroupingTypeAttribute that is package protected and won't
                // serialize.
                flattened.put(entry.getKey() + "." + Integer.toHexString(context).toUpperCase(), (TypeAttribute) attribute.copy(), true);
            }
            // Increment the context by one.
            context++;
        }

        // Set the flattened document's visibility to the combined visibilities of each document.
        flattened.setColumnVisibility(GroupingUtils.combineVisibilities(visibilities, markingFunctions, false));
        log.trace("flattened document: {}", flattened);
        return flattened;
    }
}
