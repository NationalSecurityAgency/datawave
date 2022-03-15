package datawave.query.util;

import com.google.common.collect.Sets;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;

import java.util.HashSet;

public final class FuzzyAttributeComparator {
    
    // this is a utility and should never be instantiated
    private FuzzyAttributeComparator() {
        throw new UnsupportedOperationException();
    }
    
    public static boolean singleToSingle(Attribute existingAttribute, Attribute newAttribute) {
        return (existingAttribute.getData().equals(newAttribute.getData())
                        && existingAttribute.getColumnVisibility().equals(newAttribute.getColumnVisibility()) && existingAttribute.getTimestamp() == newAttribute
                        .getTimestamp());
    }
    
    public static boolean singleToMultiple(Attributes multipleAttributes, Attribute singleAttribute) {
        return multipleAttributes
                        .getAttributes()
                        .stream()
                        .anyMatch(existingAttribute -> {
                            return (existingAttribute.getData().equals(singleAttribute.getData())
                                            && existingAttribute.getColumnVisibility().equals(singleAttribute.getColumnVisibility()) && existingAttribute
                                            .getTimestamp() == singleAttribute.getTimestamp());
                        });
    }
    
    public static boolean multipleToMultiple(Attributes existingAttributes, Attributes newAttributes) {
        boolean containsMatch = false;
        for (Attribute<? extends Comparable<?>> newAttr : newAttributes.getAttributes()) {
            boolean tempMatch = existingAttributes
                            .getAttributes()
                            .stream()
                            .anyMatch(existingAttribute -> {
                                return (existingAttribute.getData().equals(newAttr.getData())
                                                && existingAttribute.getColumnVisibility().equals(newAttr.getColumnVisibility()) && existingAttribute
                                                .getTimestamp() == newAttr.getTimestamp());
                            });
            if (tempMatch) {
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
     * @param isToKeep
     *            Flag set from the overarching Document
     * @param trackSizes
     *            Flag set from the overarching Document
     * @return
     */
    public static Attributes combineMultipleAttributes(Attributes existingAttributes, Attributes newAttributes, boolean isToKeep, boolean trackSizes) {
        HashSet<Attribute<? extends Comparable<?>>> combinedSet = Sets.newHashSet();
        
        existingAttributes.getAttributes().forEach(
                        existingAttr -> {
                            boolean containsMatch = false;
                            for (Attribute<? extends Comparable<?>> newAttr : newAttributes.getAttributes()) {
                                if (existingAttr.getData().equals(newAttr.getData())
                                                && existingAttr.getColumnVisibility().equals(newAttr.getColumnVisibility())
                                                && existingAttr.getTimestamp() == newAttr.getTimestamp()) {
                                    newAttr.setMetadata(existingAttr.getMetadata());
                                    combinedSet.add(newAttr);
                                    containsMatch = true;
                                }
                            }
                            if (!containsMatch) {
                                combinedSet.add(existingAttr);
                            }
                        });
        
        Attributes mergedAttrs = new Attributes(combinedSet, isToKeep, trackSizes);
        
        return mergedAttrs;
    }
    
    public static Attribute combineSingleAttributes(Attribute existingAttribute, Attribute newAttribute) {
        newAttribute.setMetadata(existingAttribute.getMetadata());
        
        return newAttribute;
    }
    
}
