package datawave.query.util;

import com.google.common.collect.Sets;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;

import java.util.HashSet;
import java.util.Set;

public final class FuzzyAttributeComparator {
    
    // this is a utility and should never be instantiated
    private FuzzyAttributeComparator() {
        throw new UnsupportedOperationException();
    }
    
    public static boolean singleToSingle(final Attribute existingAttribute, final Attribute newAttribute) {
        return (existingAttribute.getData().equals(newAttribute.getData())
                        && existingAttribute.getColumnVisibility().equals(newAttribute.getColumnVisibility()) && existingAttribute.getTimestamp() == newAttribute
                        .getTimestamp());
    }
    
    public static boolean singleToMultiple(final Attributes multipleAttributes, final Attribute singleAttribute) {
        return multipleAttributes.getAttributes().stream().anyMatch(existingAttribute -> {
            return singleToSingle(existingAttribute, singleAttribute);
        });
    }
    
    public static boolean multipleToMultiple(final Attributes existingAttributes, final Attributes newAttributes) {
        boolean containsMatch = false;
        for (Attribute<? extends Comparable<?>> newAttr : newAttributes.getAttributes()) {
            if (singleToMultiple(existingAttributes, newAttr)) {
                containsMatch = true;
            }
        }
        return containsMatch;
    }
    
    /**
     * Performs a basic fuzzy match on the content/data of two given Attributes. If such a match is found, combine the metadata from the two Attributes and
     * create a new set of deduplicated Attributes to return to the Document.
     *
     * @param existingAttributes
     *            The set of Attributes against which we want to compare
     * @param newAttributes
     *            The set of Attributes against which we want to check
     * @return
     */
    public static Set<Attribute<? extends Comparable<?>>> combineMultipleAttributes(final Attributes existingAttributes, final Attributes newAttributes) {
        HashSet<Attribute<? extends Comparable<?>>> combinedSet = Sets.newHashSet();
        
        existingAttributes.getAttributes().forEach(existingAttr -> {
            boolean containsMatch = false;
            for (Attribute<? extends Comparable<?>> newAttr : newAttributes.getAttributes()) {
                if (singleToSingle(existingAttr, newAttr) && newAttr.isToKeep()) {
                    combinedSet.add(combineSingleAttributes(existingAttr, newAttr));
                    containsMatch = true;
                }
            }
            if (!containsMatch) {
                combinedSet.add(existingAttr);
            }
        });
        
        return combinedSet;
    }
    
    public static Set<Attribute<? extends Comparable<?>>> combineMultipleAttributes(final Attribute existingAttribute, final Attributes newAttributes,
                    final boolean trackSizes) {
        HashSet<Attribute<? extends Comparable<?>>> existAttrSet = Sets.newHashSet();
        existAttrSet.add(existingAttribute);
        
        return combineMultipleAttributes(new Attributes(existAttrSet, newAttributes.isToKeep(), trackSizes), newAttributes);
    }
    
    public static Set<Attribute<? extends Comparable<?>>> combineMultipleAttributes(final Attributes existingAttributes, final Attribute newAttribute,
                    final boolean trackSizes) {
        HashSet<Attribute<? extends Comparable<?>>> newAttrSet = Sets.newHashSet();
        newAttrSet.add(newAttribute);
        
        return combineMultipleAttributes(existingAttributes, new Attributes(newAttrSet, existingAttributes.isToKeep(), trackSizes));
    }
    
    public static Attribute combineSingleAttributes(final Attribute existingAttribute, final Attribute newAttribute) {
        newAttribute.setMetadata(existingAttribute.getMetadata());
        return newAttribute;
    }
    
}
