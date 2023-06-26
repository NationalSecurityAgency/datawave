package datawave.query.jexl.visitors.validate;

import com.google.common.base.Joiner;
import datawave.query.exceptions.InvalidQueryTreeException;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.visitors.JexlStringBuildingVisitor;
import datawave.query.jexl.visitors.RebuildingVisitor;
import datawave.query.jexl.visitors.TreeFlatteningRebuildingVisitor;
import org.apache.commons.jexl3.parser.JexlNode;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Validates an AST with respect to some basic assumptions. Intended to be called after a given visitor is done visiting a query tree.
 *
 * <pre>
 *     1. Proper lineage with respect to parent-child pointers
 *     2. The tree is flattened
 *     3. Junctions have two or more children
 *     4. Minimal reference expressions necessary
 *     5. Query property markers are properly structured
 * </pre>
 */
public class ASTValidator {

    private static final Logger log = Logger.getLogger(ASTValidator.class);

    private boolean isValid;
    private final List<String> reasons = new ArrayList<>();

    // toggles
    private boolean validateLineage;
    private boolean validateFlatten;
    private boolean validateJunctions;
    private boolean validateReferenceExpressions;
    private boolean validateQueryPropertyMarkers;

    public ASTValidator() {
        // empty constructor
    }

    /**
     * Determines if the provided AST meets all basic assumptions. Intended to be called outside of normal visitor validation
     *
     * @param root
     *            an arbitrary JexlNode
     * @return true if the tree is valid, otherwise throws an exception
     * @throws InvalidQueryTreeException
     *             for invalid tree
     */
    public boolean isValid(JexlNode root) throws InvalidQueryTreeException {
        return isValid(root, null, true);
    }

    /**
     * Determines if the provided AST meets all basic assumptions of validity. Intended to be called after a visitor is done visiting a query tree.
     *
     * @param root
     *            an arbitrary JexlNode
     * @param sourceVisitor
     *            the visitor calling this method
     * @return true if the AST is valid, otherwise
     * @throws InvalidQueryTreeException
     *             if the query tree is invalid
     */
    public boolean isValid(JexlNode root, String sourceVisitor) throws InvalidQueryTreeException {
        return isValid(root, sourceVisitor, true);
    }

    /**
     * Determines if the AST meets all basic assumptions of validity. Throws an exception if the AST is not valid.
     *
     * @param root
     *            an arbitrary JexlNode
     * @param sourceVisitor
     *            the visitor calling this method, may be null if called outside normal scope
     * @param failHard
     *            boolean to throw an exception if the tree fails validation
     * @return true if the AST is valid, otherwise throws an exception
     * @throws InvalidQueryTreeException
     *             if the query tree is invalid
     */
    public boolean isValid(JexlNode root, String sourceVisitor, boolean failHard) throws InvalidQueryTreeException {

        log.info("Validating tree after visiting " + sourceVisitor);

        isValid = true;
        reasons.clear();

        validateLineage(root);
        validateFlatten(root);
        validateJunctions(root);
        validateReferenceExpressionsValid(root);
        validateQueryPropertyMarkers(root);

        if (!isValid) {
            String joined = "[" + Joiner.on(',').join(reasons) + "]";
            log.error(sourceVisitor + " produced an invalid query tree for reasons " + joined);
            if (failHard) {
                if (sourceVisitor != null) {
                    throw new InvalidQueryTreeException(sourceVisitor + " produced an invalid query tree: " + joined);
                } else {
                    throw new InvalidQueryTreeException("Invalid query tree detected outside of normal scope: " + joined);
                }
            }
        }
        return isValid;
    }

    /**
     * Validate the lineage of all nodes under this root
     *
     * @param root
     *            an arbitrary JexlNode
     */
    private void validateLineage(JexlNode root) {
        // do not fail hard
        if (validateLineage && !JexlASTHelper.validateLineage(root, false)) {
            isValid = false;
            reasons.add("Lineage");
        }
    }

    /**
     * Determine if this AST is flattened
     *
     * @param root
     *            an arbitrary JexlNode
     */
    private void validateFlatten(JexlNode root) {
        if (validateFlatten) {
            JexlNode copy = RebuildingVisitor.copy(root);
            String original = JexlStringBuildingVisitor.buildQueryWithoutParse(copy);
            String flattened = JexlStringBuildingVisitor.buildQueryWithoutParse(TreeFlatteningRebuildingVisitor.flatten(copy));
            if (!original.equals(flattened)) {
                isValid = false;
                reasons.add("Flatten");
            }
        }
    }

    /**
     * Determine if this AST is valid with respect to And/Or nodes and the number of children
     *
     * @param node
     *            an arbitrary JexlNode
     */
    private void validateJunctions(JexlNode node) {
        if (validateJunctions && !JunctionValidatingVisitor.validate(node)) {
            isValid = false;
            reasons.add("Junctions");
        }
    }

    /**
     * Validate the AST with respect to minimal reference expressions
     *
     * @param node
     *            an arbitrary JexlNode
     */
    private void validateReferenceExpressionsValid(JexlNode node) {
        if (validateReferenceExpressions && !MinimalReferenceExpressionsVisitor.validate(node)) {
            isValid = false;
            reasons.add("RefExpr");
        }
    }

    /**
     * Validate that all query property markers in this AST are properly structured
     *
     * @param node
     *            an arbitrary JexlNode
     */
    private void validateQueryPropertyMarkers(JexlNode node) {
        if (validateQueryPropertyMarkers && !ValidQueryPropertyMarkerVisitor.validate(node).isValid()) {
            isValid = false;
            reasons.add("Markers");
        }
    }

    public boolean isValidateLineage() {
        return validateLineage;
    }

    public void setValidateLineage(boolean validateLineage) {
        this.validateLineage = validateLineage;
    }

    public boolean getValidateFlatten() {
        return validateFlatten;
    }

    public void setValidateFlatten(boolean validateFlatten) {
        this.validateFlatten = validateFlatten;
    }

    public boolean getValidateJunctions() {
        return validateJunctions;
    }

    public void setValidateJunctions(boolean validateJunctions) {
        this.validateJunctions = validateJunctions;
    }

    public boolean getValidateReferenceExpressions() {
        return validateReferenceExpressions;
    }

    public void setValidateReferenceExpressions(boolean validateReferenceExpressions) {
        this.validateReferenceExpressions = validateReferenceExpressions;
    }

    public boolean getValidateQueryPropertyMarkers() {
        return validateQueryPropertyMarkers;
    }

    public void setValidateQueryPropertyMarkers(boolean validateQueryPropertyMarkers) {
        this.validateQueryPropertyMarkers = validateQueryPropertyMarkers;
    }
}
