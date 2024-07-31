package datawave.query.jexl.visitors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.fail;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.lang.model.type.UnionType;

import org.apache.commons.jexl3.parser.ASTAndNode;
import org.apache.commons.jexl3.parser.ASTEQNode;
import org.apache.commons.jexl3.parser.ASTJexlScript;
import org.apache.commons.jexl3.parser.ASTNENode;
import org.apache.commons.jexl3.parser.ASTNRNode;
import org.apache.commons.jexl3.parser.ASTNotNode;
import org.apache.commons.jexl3.parser.ASTOrNode;
import org.apache.commons.jexl3.parser.JexlNode;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;

import datawave.data.type.LcType;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.jexl.util.JexlQueryGenerator;
import datawave.query.util.TypeMetadata;

/**
 * A suite of integration style tests for the {@link IngestTypeVisitor}.
 * <p>
 * The following permutations are tested
 * <ul>
 * <li>unions, intersections, mix of both</li>
 * <li>equality nodes, complex nodes, marker nodes, etc</li>
 * <li>TypeMetadata that is fully inclusive, partially inclusive, partially exclusive, fully exclusive</li>
 * </ul>
 * <p>
 * Negations and null literals are tested separately due to being exceptional cases.
 */
class IngestTypeVisitorIntegrationTest {

    private final static Logger log = LoggerFactory.getLogger(IngestTypeVisitorIntegrationTest.class);

    private JexlQueryGenerator generator;
    private final Set<String> fields = Set.of("A", "B", "C", "D", "E");
    private final Set<String> values = Set.of("v1", "v2", "v3", "v4", "v5", "v6");

    private final int maxIterations = 1_000;
    private final int minTerms = 1;
    private final int maxTerms = 10;
    private boolean assertionEnabled = true;

    private final UnionTypes unionOperand = new UnionTypes();
    private final UnionTypes unionNullOperand = new UnionNullTypes();
    private final IntersectTypes intersectOperand = new IntersectTypes();
    private final IntersectNullTypes intersectNullOperand = new IntersectNullTypes();

    @BeforeEach
    public void setup() {
        generator = new JexlQueryGenerator(fields, values);
        generator.disableAllOptions();
        generator.disableIntersections();
        generator.disableUnions();

        assertionEnabled = true;
    }

    @Test
    void testUnions() {
        generator.enableUnions();
        log.info("full inclusive");
        drive(getFullyInclusiveTypeMetadata(), unionOperand);

        log.info("partial inclusive");
        drive(getPartiallyInclusiveTypeMetadata(), unionOperand);

        log.info("partial exclusive");
        drive(getPartiallyExclusiveTypeMetadata(), unionOperand);

        log.info("full exclusive");
        drive(getFullyExclusiveTypeMetadata(), unionOperand);
    }

    @Test
    void testIntersections() {
        generator.enableIntersections();
        log.info("full inclusive");
        drive(getFullyInclusiveTypeMetadata(), intersectOperand);

        log.info("partial inclusive");
        drive(getPartiallyInclusiveTypeMetadata(), intersectOperand);

        log.info("partial exclusive");
        drive(getPartiallyExclusiveTypeMetadata(), intersectOperand);

        log.info("full exclusive");
        drive(getFullyExclusiveTypeMetadata(), intersectOperand);
    }

    @Test
    void testEdgeCase_01() {
        String query = "(A == 'v5' && content:scoredPhrase(B, -1.5, termOffsetMap, 'v5', 'v1'))";
        assertTypes(query, getFullyInclusiveTypeMetadata(), intersectOperand);
    }

    @Test
    void testEdgeCase_02() {
        String query = "(filter:matchesAtLeastCountOf(1,B,v1) || E == 'v4')";
        assertTypes(query, getPartiallyInclusiveTypeMetadata(), unionOperand);
    }

    @Test
    void testEdgeCase_03() {
        String query = "(filter:matchesAtLeastCountOf(1,C,v1) || f:count(D,E,A))";
        assertTypes(query, getPartiallyInclusiveTypeMetadata(), unionOperand);
    }

    /**
     * Enable several generator options drive a bunch of queries through
     *
     * @param metadata
     *            a TypeMetadata instance
     * @param operand
     *            a TypeOperand
     */
    private void drive(TypeMetadata metadata, TypeOperand operand) {
        driveInner(metadata, operand);

        log.info("enabling regexes");
        generator.enableRegexes();
        driveInner(metadata, operand);

        log.info("enabling content functions");
        generator.enableContentFunctions();
        driveInner(metadata, operand);

        log.info("enabling filter functions");
        generator.enableFilterFunctions();
        driveInner(metadata, operand);

        log.info("enable grouping functions");
        generator.enableGroupingFunctions();
        driveInner(metadata, operand);

        if (operand instanceof UnionTypes) {
            log.info("enable multi fielded functions for unions");
            generator.enableMultiFieldedFunctions();
            driveInner(metadata, operand);
            generator.disableMultiFieldedFunctions();
        }

        log.info("enable null literals");
        generator.enableNullLiterals();
        if (operand instanceof IntersectTypes) {
            driveInner(metadata, intersectNullOperand);
        } else if (operand instanceof UnionTypes) {
            driveInner(metadata, unionNullOperand);
        }
        generator.disableNullLiterals();

        log.info("enable negations");
        generator.enableNegations();
        if (operand instanceof IntersectTypes) {
            driveInner(metadata, intersectNullOperand);
        } else if (operand instanceof UnionType) {
            driveInner(metadata, unionNullOperand);
        }
        generator.disableNegations();

        generator.disableAllOptions();
    }

    private void driveInner(TypeMetadata metadata, TypeOperand operand) {
        for (int i = 0; i < maxIterations; i++) {
            String query = generator.getQuery(minTerms, maxTerms);
            assertTypes(query, metadata, operand);
        }
    }

    private void assertTypes(String query, TypeMetadata metadata, TypeOperand operand) {
        JexlNode node = parseQuery(query);
        Set<String> types = IngestTypeVisitor.getIngestTypes(node, metadata);

        Set<String> queryTypes = operand.apply(node, metadata, this.fields);
        assertEquals(queryTypes, types, "Bad assertion for query: " + query);
    }

    @Test
    void testIntersectionOfUnions() {
        generator.enableUnions();
        driveIntersectionOfUnions(getFullyInclusiveTypeMetadata());
        driveIntersectionOfUnions(getPartiallyInclusiveTypeMetadata());
        driveIntersectionOfUnions(getPartiallyExclusiveTypeMetadata());
        driveIntersectionOfUnions(getFullyExclusiveTypeMetadata());
    }

    @Test
    void testFullRandom() {
        generator.enableAllOptions();
        generator.disableIntersections();
        generator.enableUnions();
        assertionEnabled = false;

        driveIntersectionOfUnions(getFullyInclusiveTypeMetadata());
        driveIntersectionOfUnions(getPartiallyInclusiveTypeMetadata());
        driveIntersectionOfUnions(getPartiallyExclusiveTypeMetadata());
        driveIntersectionOfUnions(getFullyExclusiveTypeMetadata());
    }

    private void driveIntersectionOfUnions(TypeMetadata metadata) {
        driveIntersectionsOfUnionsInner(metadata);

        log.info("enabling regexes");
        generator.enableRegexes();
        driveIntersectionsOfUnionsInner(metadata);

        log.info("enabling content functions");
        generator.enableContentFunctions();
        driveIntersectionsOfUnionsInner(metadata);

        log.info("enabling filter functions");
        generator.enableFilterFunctions();
        driveIntersectionsOfUnionsInner(metadata);

        log.info("enabling grouping functions");
        generator.enableGroupingFunctions();
        driveIntersectionsOfUnionsInner(metadata);

        log.info("enabling multi fielded functions");
        generator.enableMultiFieldedFunctions();
        driveIntersectionsOfUnionsInner(metadata);
    }

    private void driveIntersectionsOfUnionsInner(TypeMetadata metadata) {
        for (int i = 0; i < maxIterations; i++) {
            String leftQuery = generator.getQuery(minTerms, maxTerms);
            String rightQuery = generator.getQuery(minTerms, maxTerms);
            assertComplexQuery(leftQuery, rightQuery, metadata);
        }
    }

    private void assertComplexQuery(String leftQuery, String rightQuery, TypeMetadata metadata) {
        String fullQuery = leftQuery + " && " + rightQuery;

        JexlNode node = parseQuery(fullQuery);
        Set<String> queryTypes = IngestTypeVisitor.getIngestTypes(node, metadata);

        if (!assertionEnabled) {
            // verify no odd cases when the visitor operates on the query
            return;
        }

        JexlNode leftNode = parseQuery(leftQuery);
        JexlNode rightNode = parseQuery(rightQuery);
        Set<String> expectedTypes = new HashSet<>();

        if (generator.isNegationsEnabled() && (allNegated(leftNode) || allNegated(rightNode))) {
            // only add types for non-negated branches
            if (!allNegated((leftNode))) {
                expectedTypes.addAll(getFieldsAndTypes(leftNode, metadata).values());
            }

            if (!allNegated(rightNode)) {
                expectedTypes.addAll(getFieldsAndTypes(rightNode, metadata).values());
            }
        } else {
            expectedTypes.addAll(getFieldsAndTypes(leftNode, metadata).values());
            expectedTypes.retainAll(getFieldsAndTypes(rightNode, metadata).values());
        }

        assertEquals(expectedTypes, queryTypes, "Query: " + fullQuery);
    }

    private JexlNode parseQuery(String query) {
        try {
            return JexlASTHelper.parseAndFlattenJexlQuery(query);
        } catch (Exception e) {
            fail("Failed to parse query: " + query);
            throw new IllegalArgumentException("Bad query: " + query);
        }
    }

    private Multimap<String,String> getFieldsAndTypes(JexlNode node, TypeMetadata metadata) {
        Multimap<String,String> map = HashMultimap.create();
        Set<String> queryFields = JexlASTHelper.getIdentifierNames(node);
        queryFields = Sets.intersection(queryFields, fields); // don't pick up any extras by mistake

        for (String queryField : queryFields) {
            Set<String> typesForField = metadata.getDataTypesForField(queryField);
            map.putAll(queryField, typesForField);
        }
        return map;
    }

    private boolean allNegated(JexlNode node) {
        for (int i = 0; i < node.jjtGetNumChildren(); i++) {
            JexlNode child = node.jjtGetChild(i);
            if (!(child instanceof ASTNotNode || child instanceof ASTNENode || child instanceof ASTNRNode)) {
                return false;
            }

            // check for FIELD == null
            if (child instanceof ASTEQNode && JexlASTHelper.getLiteralValueSafely(child) == null) {
                return false;
            }
        }
        return true;
    }

    // all fields map to the same type
    private TypeMetadata getFullyInclusiveTypeMetadata() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type1", LcType.class.getTypeName());
        metadata.put("B", "type1", LcType.class.getTypeName());
        metadata.put("C", "type1", LcType.class.getTypeName());
        metadata.put("D", "type1", LcType.class.getTypeName());
        metadata.put("E", "type1", LcType.class.getTypeName());
        return metadata;
    }

    // sliding start of ingest types, all overlap
    private TypeMetadata getPartiallyInclusiveTypeMetadata() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type1", LcType.class.getTypeName());
        metadata.put("A", "type2", LcType.class.getTypeName());
        metadata.put("A", "type3", LcType.class.getTypeName());
        metadata.put("A", "type4", LcType.class.getTypeName());
        metadata.put("A", "type5", LcType.class.getTypeName());
        metadata.put("B", "type2", LcType.class.getTypeName());
        metadata.put("B", "type3", LcType.class.getTypeName());
        metadata.put("B", "type4", LcType.class.getTypeName());
        metadata.put("B", "type5", LcType.class.getTypeName());
        metadata.put("C", "type3", LcType.class.getTypeName());
        metadata.put("C", "type4", LcType.class.getTypeName());
        metadata.put("C", "type5", LcType.class.getTypeName());
        metadata.put("D", "type4", LcType.class.getTypeName());
        metadata.put("D", "type5", LcType.class.getTypeName());
        metadata.put("E", "type5", LcType.class.getTypeName());
        return metadata;
    }

    // sliding window of ingest types
    private TypeMetadata getPartiallyExclusiveTypeMetadata() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type1", LcType.class.getTypeName());
        metadata.put("A", "type2", LcType.class.getTypeName());
        metadata.put("A", "type3", LcType.class.getTypeName());
        metadata.put("B", "type2", LcType.class.getTypeName());
        metadata.put("B", "type3", LcType.class.getTypeName());
        metadata.put("B", "type4", LcType.class.getTypeName());
        metadata.put("C", "type3", LcType.class.getTypeName());
        metadata.put("C", "type4", LcType.class.getTypeName());
        metadata.put("C", "type5", LcType.class.getTypeName());
        metadata.put("D", "type4", LcType.class.getTypeName());
        metadata.put("D", "type5", LcType.class.getTypeName());
        metadata.put("D", "type6", LcType.class.getTypeName());
        metadata.put("E", "type5", LcType.class.getTypeName());
        metadata.put("E", "type6", LcType.class.getTypeName());
        metadata.put("E", "type1", LcType.class.getTypeName());
        return metadata;
    }

    // no field maps to the same type
    private TypeMetadata getFullyExclusiveTypeMetadata() {
        TypeMetadata metadata = new TypeMetadata();
        metadata.put("A", "type1", LcType.class.getTypeName());
        metadata.put("B", "type2", LcType.class.getTypeName());
        metadata.put("C", "type3", LcType.class.getTypeName());
        metadata.put("D", "type4", LcType.class.getTypeName());
        metadata.put("E", "type5", LcType.class.getTypeName());
        return metadata;
    }

    private interface TypeOperand {
        Set<String> apply(JexlNode node, TypeMetadata metadata, Set<String> fields);
    }

    private static class UnionTypes implements TypeOperand {

        @Override
        public Set<String> apply(JexlNode node, TypeMetadata metadata, Set<String> fields) {
            Set<String> types = new HashSet<>();
            Set<String> queryFields = JexlASTHelper.getIdentifierNames(node);
            queryFields = Sets.intersection(queryFields, fields);
            for (String queryField : queryFields) {
                types.addAll(metadata.getDataTypesForField(queryField));
            }
            return types;
        }
    }

    private static class IntersectTypes implements TypeOperand {

        @Override
        public Set<String> apply(JexlNode node, TypeMetadata metadata, Set<String> fields) {
            Set<String> types = new HashSet<>();
            Set<String> queryFields = JexlASTHelper.getIdentifierNames(node);
            queryFields = Sets.intersection(queryFields, fields);
            for (String queryField : queryFields) {
                if (types.isEmpty()) {
                    types.addAll(metadata.getDataTypesForField(queryField));
                } else {
                    types.retainAll(metadata.getDataTypesForField(queryField));
                    if (types.isEmpty()) {
                        return new HashSet<>();
                    }
                }
            }
            return types;
        }
    }

    /**
     * Handles null, not null, and negated terms
     */
    private static class IntersectNullTypes extends IntersectTypes {

        @Override
        public Set<String> apply(JexlNode node, TypeMetadata metadata, Set<String> fields) {
            JexlNode flattened = TreeFlatteningRebuildingVisitor.flatten(node);
            assertInstanceOf(ASTJexlScript.class, flattened);

            JexlNode and = JexlASTHelper.dereference(flattened.jjtGetChild(0));
            List<JexlNode> children = new LinkedList<>();

            if (!(and instanceof ASTAndNode)) {
                // add single leaf
                children.add(JexlASTHelper.dereference(and));
            } else {
                for (int i = 0; i < and.jjtGetNumChildren(); i++) {
                    children.add(JexlASTHelper.dereference(and.jjtGetChild(i)));
                }
            }

            // extract types
            boolean intersectedToZero = false;
            Set<String> types = new HashSet<>();
            for (JexlNode child : children) {
                Set<String> nodeFields = JexlASTHelper.getIdentifierNames(child);
                Set<String> nodeTypes = new HashSet<>();
                for (String nodeField : nodeFields) {
                    nodeTypes.addAll(metadata.getDataTypesForField(nodeField));
                }

                if (isNullEquality(child) || isNotNullEquality(child) || isNegated(child)) {
                    continue;
                }

                if (types.isEmpty()) {
                    types.addAll(nodeTypes);
                } else {
                    types.retainAll(nodeTypes);
                    if (types.isEmpty()) {
                        intersectedToZero = true;
                        break;
                    }
                }
            }

            if (!intersectedToZero && types.isEmpty()) {
                types.add(IngestTypeVisitor.IGNORED_TYPE);
            }

            return types;
        }
    }

    /**
     * Handles null, not null, and negated terms
     */
    private static class UnionNullTypes extends UnionTypes {

        @Override
        public Set<String> apply(JexlNode node, TypeMetadata metadata, Set<String> fields) {
            JexlNode flattened = TreeFlatteningRebuildingVisitor.flatten(node);
            assertInstanceOf(ASTJexlScript.class, flattened);

            JexlNode or = JexlASTHelper.dereference(flattened.jjtGetChild(0));
            List<JexlNode> children = new LinkedList<>();

            if (!(or instanceof ASTOrNode)) {
                // dealing with a leaf, add it
                children.add(JexlASTHelper.dereference(or));
            } else {
                for (int i = 0; i < or.jjtGetNumChildren(); i++) {
                    JexlNode child = or.jjtGetChild(i);
                    child = JexlASTHelper.dereference(child);
                    children.add(child);
                }
            }

            // extract types
            Set<String> types = new HashSet<>();
            for (JexlNode child : children) {
                // when performing an internal visit of a union, skip null and not null equality nodes
                if (!isNullEquality(child) && !isNotNullEquality(child) && !isNegated(child)) {
                    Set<String> nodeFields = JexlASTHelper.getIdentifierNames(child);
                    for (String nodeField : nodeFields) {
                        types.addAll(metadata.getDataTypesForField(nodeField));
                    }
                }
            }

            if (types.isEmpty()) {
                // if no node returned a type, then this whole union can be ignored
                types.add(IngestTypeVisitor.IGNORED_TYPE);
            }
            return types;
        }
    }

    private static boolean isNullEquality(JexlNode node) {
        node = JexlASTHelper.dereference(node);

        if (node instanceof ASTEQNode) {
            return JexlASTHelper.getLiteralValueSafely(node) == null;
        }

        return false;
    }

    private static boolean isNotNullEquality(JexlNode node) {
        node = JexlASTHelper.dereference(node);

        if (node instanceof ASTNotNode) {
            node = node.jjtGetChild(0);
            node = JexlASTHelper.dereference(node);
        } else {
            return false;
        }

        if (node instanceof ASTEQNode) {
            return JexlASTHelper.getLiteralValueSafely(node) == null;
        }

        return false;
    }

    private static boolean isNegated(JexlNode node) {
        node = JexlASTHelper.dereference(node);
        return node instanceof ASTNotNode;
    }
}
