package datawave.query.model.edge;

import java.util.Collection;
import java.util.Map.Entry;

import datawave.edge.model.DefaultEdgeModelFieldsFactory;
import datawave.edge.model.EdgeModelFields;
import datawave.edge.model.EdgeModelFieldsFactory;
import datawave.query.model.QueryModel;
import datawave.query.model.util.LoadModel;

/**
 * This class defines a typical QueryModel, allowing the query syntax for edge queries to be easily customized for an external client's needs/preferences.
 * However, it has a few important distinctions from event-based QueryModel definitions:
 *
 * <br>
 * <br>
 * (1) Unlike event-based query models, edge field names don't exist on disk in the way that internal event attributes do. The edge data model is relatively
 * simple and static with respect to the set of all possible field names. And because we're not constrained by the physical representation of field names on
 * disk, we have the flexibility to choose an internal naming scheme to suit the targeted deployment environment. See {@link EdgeModelFields}. For example, with
 * respect to the superclass method {@code addTermToModel(String alias, String nameOnDisk)}, 'nameOnDisk' can be whatever we want and can be managed with
 * configuration as needed. <br>
 * <br>
 * (2) Validation: Only 1-to-1 relationships are allowed within the query model. Each alias must have at most one 'nameOnDisk' value for a given forward field
 * mapping. Otherwise the query model is considered to be invalid. Moreover, validation performed by this class will ensure that each 'nameOnDisk' value
 * referenced by the query model exists within the configured internal data model, ie, within the set returned by the {@code getAllInternalFieldNames} method,
 * via case-sensitive match. <br>
 * <br>
 * (3) Additionally, index-only/unevaluated fields are ignored, as this concept is not applicable to edges.
 */
public class EdgeQueryModel extends QueryModel {

    private final EdgeModelFields fields;

    /**
     * This constructor allows the class to be used in conjunction with existing QueryModel loaders.
     *
     * @param other
     *            the other model
     * @throws InvalidModelException
     *             if the model is invalid
     */
    public EdgeQueryModel(QueryModel other, EdgeModelFields fields) throws InvalidModelException {
        super(other);
        this.fields = fields;
        validateModel(this);
    }

    public EdgeModelFields getFields() {
        return fields;
    }

    /**
     * Simple factory method to load a query model from the specified classpath resource.
     *
     * @param queryModelXml
     *            the model xml
     * @return EdgeQueryModel instance
     * @throws Exception
     *             if there is an issue
     */
    public static EdgeQueryModel loadModel(String queryModelXml, EdgeModelFields fields) throws Exception {
        return new EdgeQueryModel(LoadModel.loadModelFromXml(queryModelXml), fields);
    }

    /**
     * Simple factory method to load a query model from the specified classpath resource.
     *
     * @return EdgeQueryModel instance
     */
    public static EdgeQueryModel loadModel(String queryModelXml, EdgeModelFieldsFactory fieldsFactory) throws Exception {
        return loadModel(queryModelXml, fieldsFactory.createFields());
    }

    /**
     * Simple factory method to load a query model from the specified classpath resource.
     *
     * @return EdgeQueryModel instance
     */
    public static EdgeQueryModel loadModel(String queryModelXml) throws Exception {
        return loadModel(queryModelXml, new DefaultEdgeModelFieldsFactory());
    }

    /**
     * Thrown whenever an invalid edge query model is detected.
     */
    public static class InvalidModelException extends Exception {
        private static final long serialVersionUID = 1L;

        public InvalidModelException() {
            super();
        }

        public InvalidModelException(String message, Throwable cause) {
            super(message, cause);
        }

        public InvalidModelException(String message) {
            super(message);
        }

        public InvalidModelException(Throwable cause) {
            super(cause);
        }
    }

    /**
     * Ensures that the given model contains valid 1-to-1 mappings of alias to internal field.
     *
     * @param model
     *            a query model
     * @throws InvalidModelException
     *             if the model is invalid
     */
    public static void validateModel(EdgeQueryModel model) throws InvalidModelException {
        if (null == model.getForwardQueryMapping()) {
            throw new InvalidModelException("Forward query mapping cannot be null");
        }
        if (model.getForwardQueryMapping().isEmpty()) {
            throw new InvalidModelException("Forward query mapping cannot be empty");
        }
        // Ensure all mappings are 1-to-1, and ensure that each alias maps to a recognized internal field name
        for (Entry<String,String> modelEntry : model.getForwardQueryMapping().entries()) {
            Collection<String> aliasedFieldNames = model.getForwardQueryMapping().get(modelEntry.getKey());
            if (null == aliasedFieldNames || aliasedFieldNames.isEmpty()) {
                throw new InvalidModelException(String.format("The model has no field mapping for alias '%s'", modelEntry.getKey()));
            }
            if (aliasedFieldNames.size() > 1) {
                throw new InvalidModelException(
                                "An alias cannot be associated with more than one internal field name. Offending alias: " + modelEntry.getKey());
            }
            for (String aliasedFieldName : aliasedFieldNames) {
                if (!model.getAllInternalFieldNames().contains(aliasedFieldName)) {
                    String msg = String.format(
                                    "The model contains a 'nameOnDisk' field that isn't recognized by the internal model. " + "Alias: '%s' Invalid Field: '%s'",
                                    modelEntry.getKey(), aliasedFieldName);
                    throw new InvalidModelException(msg);
                }
            }
        }
    }

    public Collection<String> getAllInternalFieldNames() {
        return fields.getBaseFieldNames();
    }
}
