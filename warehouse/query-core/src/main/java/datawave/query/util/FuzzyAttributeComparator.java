package datawave.query.util;

import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;

public final class FuzzyAttributeComparator {
    
    // this is a utility and should never be instantiated
    private FuzzyAttributeComparator() {
        throw new UnsupportedOperationException();
    }
    
    public static boolean singleToSingle(Attribute existingAttribute, Attribute newAttribute) {
        return existingAttribute.getData().equals(newAttribute.getData());
    }
    
    public static boolean singleToMultiple(Attributes multipleAttributes, Attribute singleAttribute) {
        return multipleAttributes.getAttributes().stream().anyMatch(existingAttribute -> {
            return existingAttribute.getData().equals(singleAttribute.getData());
        });
    }
    
    public static boolean multipleToMultiple(Attributes existingAttributes, Attributes newAttributes) {
        boolean containsMatch = false;
        for (Attribute<? extends Comparable<?>> newAttr : newAttributes.getAttributes()) {
            boolean tempMatch = existingAttributes.getAttributes().stream().anyMatch(existingAttribute -> {
                return existingAttribute.getData().equals(newAttr.getData());
            });
            if (tempMatch) {
                containsMatch = true;
            }
        }
        return containsMatch;
    }
    
    public static Attribute combineAttributes(Attributes existingAttributes, Attributes newAttributes) {
        existingAttributes.getAttributes().forEach(existAttr -> {
            for (Attribute<? extends Comparable<?>> newAttr : newAttributes.getAttributes()) {
                if (existAttr.getData().equals(newAttr.getData())) {
                    newAttr.setMetadata(existAttr.getMetadata());
                }
            }
        });
        
        newAttributes.setBytes(existingAttributes.sizeInBytes());
        newAttributes.setCount(existingAttributes.size());
        
        return newAttributes;
    }
}
