package datawave.query.function;

import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.data.Key;

import com.google.common.base.Function;
import com.google.common.collect.Sets;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Document;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.util.Tuple2;

/**
 * This is for when the user has not requested include.grouping.context, but I added it in order to process matchesInGroup. Take out the unwanted grouping
 * context before returning documents to the user.
 */
public class RemoveGroupingContext implements Function<Entry<Key,Document>,Entry<Key,Document>> {

    @Override
    public Entry<Key,Document> apply(Entry<Key,Document> entry) {
        Set<Tuple2<String,Attribute<? extends Comparable<?>>>> toRemove = Sets.newHashSet();
        for (Entry<String,Attribute<? extends Comparable<?>>> attribute : entry.getValue().entrySet()) {
            String fieldName = attribute.getKey();
            if (JexlASTHelper.hasGroupingContext(fieldName)) {
                toRemove.add(new Tuple2<>(attribute.getKey(), attribute.getValue()));
            }
        }
        // remove everyone with a grouping context
        for (Tuple2<String,Attribute<? extends Comparable<?>>> goner : toRemove) {
            entry.getValue().removeAll(goner.first());
        }
        // put them all back without the grouping context
        for (Tuple2<String,Attribute<? extends Comparable<?>>> goner : toRemove) {
            entry.getValue().put(goner.first(), goner.second(), false);
        }
        return entry;
    }
}
