package datawave.query.util.transformer;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.log4j.Logger;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NoOpType;
import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.AttributeFactory;
import datawave.query.attributes.TypeAttribute;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.model.QueryModel;
import datawave.query.util.TypeMetadata;

/**
 * Utility class that handles rebuilding a basic NoOp attribute into an attribute of the correct type, given TypeMetadata and optionally a QueryModel
 */
public class AttributeRebuilder {
    private static final Logger log = Logger.getLogger(AttributeRebuilder.class);
    private final TypeMetadata typeMetadata;
    private final Multimap<String,String> fieldMap;
    private final Map<String,Class<?>> classCache;

    /**
     * Main constructor for the rebuilder, QueryModel may be null
     *
     * @param typeMetadata
     * @param queryModel
     */
    public AttributeRebuilder(TypeMetadata typeMetadata, @Nullable QueryModel queryModel) {
        this.typeMetadata = typeMetadata;
        if (queryModel == null) {
            this.fieldMap = HashMultimap.create();
        } else {
            this.fieldMap = invertMap(queryModel.getReverseQueryMapping());
        }
        this.classCache = new HashMap<>();
    }

    private Multimap<String,String> invertMap(Map<String,String> map) {
        HashMultimap<String,String> mappings = HashMultimap.create();
        for (Map.Entry<String,String> entry : map.entrySet()) {
            mappings.put(entry.getValue(), entry.getKey());
        }
        return mappings;
    }

    /**
     * Given a field and an attribute, return the correctly typed attribute
     *
     * @param field
     *            the field
     * @param attr
     *            the attribute
     * @return an attribute of the correct type
     */
    public Attribute rebuild(String field, Attribute<?> attr) {
        field = JexlASTHelper.deconstructIdentifier(field);
        Set<String> normalizerNames = typeMetadata.getNormalizerNamesForField(field);

        if (normalizerNames.isEmpty()) {
            populateNormalizerFromQueryModel(field, normalizerNames);
        }

        // see AttributeFactory#getKeepers
        if (normalizerNames.size() > 1 && normalizerNames.contains(NoOpType.class.getName())) {
            normalizerNames.remove(NoOpType.class.getName());
        }

        if (normalizerNames.size() > 1 && normalizerNames.contains(LcNoDiacriticsType.class.getName())) {
            normalizerNames.remove(LcNoDiacriticsType.class.getName());
        }

        if (normalizerNames.size() > 1) {
            log.warn("Found more than one normalizer, using the first for field " + field + ": " + normalizerNames);
        }

        for (String name : normalizerNames) {
            try {
                Class<?> clazz = getClass(name);
                Type<?> type = (Type<?>) clazz.getDeclaredConstructor().newInstance();
                type.setDelegateFromString(String.valueOf(attr.getData()));
                return new TypeAttribute<>(type, attr.getMetadata(), attr.isToKeep());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return attr;
    }

    /**
     * Populate normalizer names from the query model
     *
     * @param field
     *            the field
     * @param normalizerNames
     *            the set of normalizer names
     */
    private void populateNormalizerFromQueryModel(String field, Set<String> normalizerNames) {
        if (log.isTraceEnabled()) {
            log.trace("Field " + field + " not found in TypeMetadata, falling back to QueryModel");
        }

        Collection<String> aliases = fieldMap.get(field);
        for (String alias : aliases) {
            normalizerNames.addAll(typeMetadata.getNormalizerNamesForField(alias));
        }
    }

    /**
     * Get the class for the provided name
     *
     * @param name
     *            the name
     * @return a class
     */
    private Class<?> getClass(String name) {
        return classCache.computeIfAbsent(name, className -> {
            try {
                return Class.forName(className);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
        });
    }
}
