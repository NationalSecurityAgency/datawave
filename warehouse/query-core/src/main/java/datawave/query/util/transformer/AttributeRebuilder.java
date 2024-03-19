package datawave.query.util.transformer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import com.google.common.collect.Multimap;

import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.TypeAttribute;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.model.QueryModel;
import datawave.query.util.TypeMetadata;

import javax.annotation.Nullable;

/**
 * Utility class that handles rebuilding a basic NoOp attribute into an attribute of the correct type, given TypeMetadata and optionally a QueryModel
 */
public class AttributeRebuilder {
    private static final Logger log = Logger.getLogger(AttributeRebuilder.class);
    private final TypeMetadata typeMetadata;
    private final QueryModel queryModel;
    private final Map<String,Class<?>> classCache;

    /**
     * Main constructor for the rebuilder, QueryModel may be null
     *
     * @param typeMetadata
     * @param queryModel
     */
    public AttributeRebuilder(TypeMetadata typeMetadata, @Nullable QueryModel queryModel) {
        this.typeMetadata = typeMetadata;
        this.queryModel = queryModel;
        this.classCache = new HashMap<>();
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

        if (normalizerNames.size() > 1 && log.isTraceEnabled()) {
            log.trace("Found " + normalizerNames.size() + " normalizers for field " + field + ", using first normalizer");
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

        if (queryModel == null) {
            log.warn("QueryModel is null, cannot populate normalizers for " + field + " from model");
            return;
        }

        // check forward mappings
        Multimap<String,String> forwardMappings = queryModel.getForwardQueryMapping();

        if (forwardMappings.keySet().contains(field)) {
            Collection<String> values = forwardMappings.get(field);
            for (String value : values) {
                normalizerNames.addAll(typeMetadata.getNormalizerNamesForField(value));
            }
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
