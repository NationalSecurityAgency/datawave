package datawave.springframework.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

import org.junit.jupiter.api.Test;

import datawave.edge.model.DefaultEdgeModelFieldsFactory;
import datawave.edge.model.EdgeModelFields;
import datawave.edge.model.EdgeModelFieldsFactory;

class SpringContextComparatorTest {

    @Test
    void ignoresDifferentSupplierImplementationsWithoutInvokingThem() {
        Supplier<String> lambda = () -> {
            throw new AssertionError("Supplier must not be invoked");
        };
        Supplier<String> anonymous = new Supplier<>() {
            @Override
            public String get() {
                throw new AssertionError("Supplier must not be invoked");
            }
        };

        assertTrue(SpringContextComparator
                        .compareObjectsRecursive("factory", new SupplierHolder(lambda), new SupplierHolder(anonymous), Collections.singleton(Supplier.class))
                        .isEmpty());
        assertTrue(SpringContextComparator
                        .compareObjectsRecursive("factory", new SupplierHolder(anonymous), new SupplierHolder(lambda), Collections.singleton(Supplier.class))
                        .isEmpty());
    }

    @Test
    void reportsMissingSupplier() {
        List<SpringContextComparator.ComparisonDifference> differences = SpringContextComparator.compareObjectsRecursive("factory", new SupplierHolder(null),
                        new SupplierHolder(() -> "principal"), Collections.singleton(Supplier.class));

        assertEquals(1, differences.size());
        assertEquals("serverUserDetailsSupplier", differences.get(0).fieldPath);
        assertTrue(differences.get(0).description.startsWith("Null mismatch:"));

        differences = SpringContextComparator.compareObjectsRecursive("factory", new SupplierHolder(() -> "principal"), new SupplierHolder(null),
                        Collections.singleton(Supplier.class));

        assertEquals(1, differences.size());
        assertEquals("serverUserDetailsSupplier", differences.get(0).fieldPath);
        assertTrue(differences.get(0).description.startsWith("Null mismatch:"));
    }

    @Test
    void reportsIncompatibleTypesInsteadOfReflectingOnTheWrongObject() {
        List<SpringContextComparator.ComparisonDifference> differences = SpringContextComparator.compareObjectsRecursive("factory", new SupplierHolder(null),
                        new DifferentHolder(), Collections.emptyList());

        assertEquals(1, differences.size());
        assertTrue(differences.get(0).description.startsWith("Runtime type mismatch:"));
    }

    @Test
    void comparesOrdinaryFields() {
        assertTrue(SpringContextComparator.compareObjectsRecursive("bean", new ValueHolder("same"), new ValueHolder("same"), Collections.emptyList())
                        .isEmpty());

        List<SpringContextComparator.ComparisonDifference> differences = SpringContextComparator.compareObjectsRecursive("bean", new ValueHolder("one"),
                        new ValueHolder("two"), Collections.emptyList());

        assertEquals(1, differences.size());
        assertEquals("value", differences.get(0).fieldPath);
        assertEquals("Value mismatch", differences.get(0).description);
    }

    @Test
    void comparesEdgeFactoriesThroughTheirInterface() {
        EdgeModelFieldsFactory defaultFactory = new DefaultEdgeModelFieldsFactory() {
            @Override
            public EdgeModelFields createFields() {
                return edgeFields("SOURCE", "GROUPED", "COUNT");
            }
        };
        EdgeModelFieldsFactory otherFactory = () -> edgeFields("SOURCE", "GROUPED", "COUNT");

        assertTrue(SpringContextComparator.compareObjectsRecursive("factory", defaultFactory, otherFactory, Collections.emptyList()).isEmpty());
        assertTrue(SpringContextComparator.compareObjectsRecursive("factory", otherFactory, defaultFactory, Collections.emptyList()).isEmpty());
    }

    @Test
    void detectsDifferencesInAllEdgeFieldMaps() {
        EdgeModelFieldsFactory factory = () -> edgeFields("SOURCE", "GROUPED", "COUNT");
        EdgeModelFieldsFactory differentFactory = () -> edgeFields("OTHER_SOURCE", "OTHER_GROUPED", "OTHER_COUNT");

        List<SpringContextComparator.ComparisonDifference> differences = SpringContextComparator.compareObjectsRecursive("factory", factory, differentFactory,
                        Collections.emptyList());

        for (String map : List.of("baseFieldMap", "keyUtilFieldMap", "transformFieldMap")) {
            assertTrue(differences.stream().anyMatch(d -> d.fieldPath.startsWith("createFields()." + map + "[")), map);
        }
    }

    @Test
    void reportsEdgeFactoryFailures() {
        EdgeModelFieldsFactory factory = () -> {
            throw new IllegalStateException("Cannot create edge fields");
        };
        EdgeModelFieldsFactory otherFactory = () -> edgeFields("SOURCE", "GROUPED", "COUNT");

        List<SpringContextComparator.ComparisonDifference> differences = SpringContextComparator.compareObjectsRecursive("factory", factory, otherFactory,
                        Collections.emptyList());

        assertEquals(1, differences.size());
        assertTrue(differences.get(0).description.contains("Cannot create edge fields"));
    }

    private static EdgeModelFields edgeFields(String source, String grouped, String count) {
        EdgeModelFields fields = new EdgeModelFields();
        fields.setBaseFieldMap(Collections.singletonMap("EDGE_SOURCE", source));
        fields.setKeyUtilFieldMap(Collections.singletonMap("GROUPED_FIELDS", grouped));
        fields.setTransformFieldMap(Collections.singletonMap("COUNT", count));
        return fields;
    }

    private static class SupplierHolder {
        private final Supplier<String> serverUserDetailsSupplier;

        private SupplierHolder(Supplier<String> supplier) {
            this.serverUserDetailsSupplier = supplier;
        }
    }

    private static class DifferentHolder {
        private final String value = "different";
    }

    private static class ValueHolder {
        private final String value;

        private ValueHolder(String value) {
            this.value = value;
        }
    }
}
