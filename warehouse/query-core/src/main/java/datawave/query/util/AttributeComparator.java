package datawave.query.util;

import com.google.common.collect.Sets;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;

import java.util.HashSet;
import java.util.Set;

public final class AttributeComparator {
    
    // this is a utility and should never be instantiated
    private AttributeComparator() {
        throw new UnsupportedOperationException();
    }
    
    public static boolean singleToSingle(final Attribute attr1, final Attribute attr2) {
        return (attr1.getData().equals(attr2.getData()) && attr1.getColumnVisibility().equals(attr2.getColumnVisibility()) && attr1.getTimestamp() == attr2
                        .getTimestamp());
    }
    
    public static boolean singleToMultiple(final Attribute attr, final Attributes attrs) {
        return attrs.getAttributes().stream().anyMatch(existingAttribute -> {
            return singleToSingle(existingAttribute, attr);
        });
    }
    
    public static boolean multipleToMultiple(final Attributes attrs1, final Attributes attrs2) {
        boolean containsMatch = false;
        for (Attribute<? extends Comparable<?>> newAttr : attrs2.getAttributes()) {
            if (singleToMultiple(newAttr, attrs1)) {
                containsMatch = true;
            }
        }
        return containsMatch;
    }
    
    /**
     * Performs a basic match on the content/data of two given Attributes. If such a match is found, combine the metadata from the two Attributes and create new
     * deduplicated Attributes to return to the Document.
     *
     * @param attrs1
     *            The Attributes against which we want to compare
     * @param attrs2
     *            The Attributes against which we want to check
     * @return
     */
    public static Set<Attribute<? extends Comparable<?>>> combineMultipleAttributes(final Attributes attrs1, final Attributes attrs2) {
        HashSet<Attribute<? extends Comparable<?>>> combinedAttrSet = Sets.newHashSet();
        
        attrs1.getAttributes().forEach(attr1 -> {
            boolean containsMatch = false;
            for (Attribute<? extends Comparable<?>> attr2 : attrs2.getAttributes()) {
                if (singleToSingle(attr1, attr2) && attr2.isToKeep()) {
                    combinedAttrSet.add(combineSingleAttributes(attr1, attr2));
                    containsMatch = true;
                }
            }
            if (!containsMatch) {
                combinedAttrSet.add(attr1);
            }
        });
        
        return combinedAttrSet;
    }
    
    public static Set<Attribute<? extends Comparable<?>>> combineMultipleAttributes(final Attribute attr, final Attributes attrs, final boolean trackSizes) {
        HashSet<Attribute<? extends Comparable<?>>> attrSet = Sets.newHashSet();
        attrSet.add(attr);
        
        return combineMultipleAttributes(new Attributes(attrSet, attrs.isToKeep(), trackSizes), attrs);
    }
    
    public static Set<Attribute<? extends Comparable<?>>> combineMultipleAttributes(final Attributes attrs, final Attribute attr, final boolean trackSizes) {
        HashSet<Attribute<? extends Comparable<?>>> attrSet = Sets.newHashSet();
        attrSet.add(attr);
        
        return combineMultipleAttributes(attrs, new Attributes(attrSet, attrs.isToKeep(), trackSizes));
    }
    
    public static Attribute combineSingleAttributes(final Attribute attr1, final Attribute attr2) {
        Attribute mergedAttr = (Attribute) attr2.copy();
        mergedAttr.setMetadata(attr1.getMetadata());
        
        return mergedAttr;
    }
    
}
