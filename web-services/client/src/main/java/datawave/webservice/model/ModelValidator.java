package datawave.webservice.model;

import datawave.query.util.MetadataHelper;
import datawave.util.UniversalSet;
import org.apache.accumulo.core.client.TableNotFoundException;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class ModelValidator {
    
    public static void validate(MetadataHelper metadataHelper, Model model) throws InvalidModelException, TableNotFoundException {
        
        Map<String,Set<String>> forwardMappings = buildForwardMappingMap(model);
        checkFieldProperties(metadataHelper, forwardMappings);
        // anything else to check?
        
    }
    
    private static void checkFieldProperties(MetadataHelper metadataHelper, Map<String,Set<String>> forwardMappings) throws TableNotFoundException,
                    InvalidModelException {
        
        StringBuilder messages = new StringBuilder();
        
        for (Map.Entry<String,Set<String>> modelField : forwardMappings.entrySet()) {
            boolean hasIndexedFields = false;
            boolean hasNonIndexedFields = false;
            
            boolean hasReverseIndexedFields = false;
            boolean hasNonReverseIndexedFields = false;
            
            boolean hasTokenizedFields = false;
            boolean hasNonTokenizedFields = false;
            
            Iterator<String> itr = modelField.getValue().iterator();
            while (itr.hasNext()) {
                String dbField = itr.next();
                if (metadataHelper.isIndexed(dbField, UniversalSet.instance())) {
                    hasIndexedFields = true;
                } else {
                    hasNonIndexedFields = true;
                }
                
                if (metadataHelper.isReverseIndexed(dbField, UniversalSet.instance())) {
                    hasReverseIndexedFields = true;
                } else {
                    hasNonReverseIndexedFields = true;
                }
                
                if (metadataHelper.isTokenized(dbField, UniversalSet.instance())) {
                    hasTokenizedFields = true;
                } else {
                    hasNonTokenizedFields = true;
                }
                
            }
            
            if (hasIndexedFields && hasNonIndexedFields) {
                messages.append("Model field ");
                messages.append(modelField.getKey());
                messages.append(" has mixed indexed states.");
                messages.append("\n");
            }
            
            if (hasReverseIndexedFields && hasNonReverseIndexedFields) {
                messages.append("Model field ");
                messages.append(modelField.getKey());
                messages.append(" has mixed reverse index states.");
                messages.append("\n");
            }
            
            if (hasTokenizedFields && hasNonTokenizedFields) {
                messages.append("Model field ");
                messages.append(modelField.getKey());
                messages.append(" has mixed tokenized states.");
                messages.append("\n");
                
            }
            
        }
        
        if (messages.length() > 0) {
            throw new InvalidModelException(messages.toString());
            
        }
        
    }
    
    private static Map<String,Set<String>> buildForwardMappingMap(Model model) {
        Map<String,Set<String>> map = new HashMap<>();
        Iterator<FieldMapping> modelFieldIter = model.getFields().iterator();
        while (modelFieldIter.hasNext()) {
            FieldMapping fm = modelFieldIter.next();
            if (fm.getDirection().equals(Direction.FORWARD)) {
                if (map.containsKey(fm.getModelFieldName())) {
                    Set<String> dbFields = new HashSet<>(map.get(fm.getModelFieldName()));
                    dbFields.add(fm.getFieldName());
                    map.put(fm.getModelFieldName(), dbFields);
                } else {
                    map.put(fm.getModelFieldName(), Collections.singleton(fm.getFieldName()));
                }
            }
            
        }
        return map;
    }
    
    public static class InvalidModelException extends Exception {
        
        public InvalidModelException(String message) {
            super(message);
        }
        
    }
}
