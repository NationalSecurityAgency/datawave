package datawave.query.common.grouping;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

import datawave.query.attributes.TemporalGranularity;

public class GroupFieldsTest {

    private static final ObjectMapper objectMapper = new ObjectMapper();
    private static final Multimap<String,String> inverseReverseModel = HashMultimap.create();
    private static final Map<String,String> reverseModel = new HashMap<>();

    @BeforeClass
    public static void beforeClass() {
        inverseReverseModel.put("GEN", "GENERE");
        inverseReverseModel.put("GEN", "GENDER");
        inverseReverseModel.put("AG", "AGE");
        inverseReverseModel.put("NOME", "NAME");
        inverseReverseModel.put("ADDR", "ADDRESS");

        reverseModel.put("GENERE", "GEN");
        reverseModel.put("GENDER", "GEN");
        reverseModel.put("AGE", "AG");
        reverseModel.put("NAME", "NOME");
        reverseModel.put("ADDRESS", "ADDR");
    }

    @Test
    public void testEmptyGroupFieldsToString() {
        GroupFields groupFields = new GroupFields();
        assertThat(groupFields.toString()).isEmpty();
    }

    @Test
    public void testGroupFieldsToString() {
        GroupFields groupFields = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("A", TemporalGranularity.ALL);
        map.put("1", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        groupFields.setGroupByFieldMap(map);
        groupFields.setSumFields(Sets.newHashSet("B", "2"));
        groupFields.setCountFields(Sets.newHashSet("C", "3"));
        groupFields.setAverageFields(Sets.newHashSet("D", "4"));
        groupFields.setMinFields(Sets.newHashSet("E", "5"));
        groupFields.setMaxFields(Sets.newHashSet("F", "6"));

        assertThat(groupFields.toString()).isEqualTo("GROUP(1[DAY],A[ALL])|SUM(B,2)|COUNT(C,3)|AVERAGE(D,4)|MIN(E,5)|MAX(F,6)");
    }

    @Test
    public void testRemappedGroupFieldsToString() {
        GroupFields groupFields = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("AG", TemporalGranularity.ALL);
        map.put("GEN", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        groupFields.setGroupByFieldMap(map);
        groupFields.setSumFields(Sets.newHashSet("AG"));
        groupFields.setCountFields(Sets.newHashSet("NOME"));
        groupFields.setAverageFields(Sets.newHashSet("AG"));
        groupFields.setMinFields(Sets.newHashSet("GEN"));
        groupFields.setMaxFields(Sets.newHashSet("NOME"));

        groupFields.remapFields(inverseReverseModel, reverseModel);

        assertThat(groupFields.toString()).isEqualTo(
                        "GROUP(AG[ALL],GEN[DAY])|SUM(AG)|COUNT(NOME)|AVERAGE(AG)|MIN(GEN)|MAX(NOME)|REVERSE_MODEL_MAP(GENERE=GEN:GENDER=GEN:AGE=AG:NAME=NOME)");
    }

    /**
     * Test the result of {@link GroupFields#from(String)} given a null string.
     */
    @Test
    public void testFromNullString() {
        assertThat(GroupFields.from(null)).isNull();
    }

    /**
     * Test the result of {@link GroupFields#from(String)} given an empty string.
     */
    @Test
    public void testFromEmptyString() {
        assertThat(GroupFields.from("")).isEqualTo(new GroupFields());
    }

    /**
     * Test the result of {@link GroupFields#from(String)} given a string with whitespace.
     */
    @Test
    public void testFromGivenWhitespace() {
        assertThat(GroupFields.from("   ")).isEqualTo(new GroupFields());
    }

    /**
     * Test the result of {@link GroupFields#from(String)} given a string with grouping fields only.
     */
    @Test
    public void testFromGivenGroupByFieldsOnly() {
        GroupFields expected = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("AGE", TemporalGranularity.ALL);
        map.put("GENDER", TemporalGranularity.ALL);
        expected.setGroupByFieldMap(map);

        GroupFields actual = GroupFields.from("GROUP(AGE,GENDER)");

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test the result of {@link GroupFields#from(String)} given a string with some aggregation fields.
     */
    @Test
    public void testFromGivenSomeAggregationFields() {
        GroupFields expected = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("AGE", TemporalGranularity.ALL);
        map.put("GENDER", TemporalGranularity.ALL);
        expected.setGroupByFieldMap(map);
        expected.setSumFields(Sets.newHashSet("AGE"));
        expected.setMaxFields(Sets.newHashSet("NAME"));

        GroupFields actual = GroupFields.from("GROUP(AGE,GENDER)|SUM(AGE)|MAX(NAME)");

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test the result of {@link GroupFields#from(String)} given a string with all aggregation fields.
     */
    @Test
    public void testFromGivenAllAggregationFields() {
        GroupFields expected = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("AGE", TemporalGranularity.ALL);
        map.put("GENDER", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        expected.setGroupByFieldMap(map);
        expected.setSumFields(Sets.newHashSet("BAT"));
        expected.setCountFields(Sets.newHashSet("FOO"));
        expected.setAverageFields(Sets.newHashSet("BAR"));
        expected.setMinFields(Sets.newHashSet("HAT"));
        expected.setMaxFields(Sets.newHashSet("BAH"));

        GroupFields actual = GroupFields.from("GROUP(AGE,GENDER[DAY])|SUM(BAT)|COUNT(FOO)|AVERAGE(BAR)|MIN(HAT)|MAX(BAH)");

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test the result of {@link GroupFields#from(String)} given a string with that contains a reverse model map.
     */
    @Test
    public void testFromGivenRemappedFields() {
        GroupFields expected = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("AG", TemporalGranularity.ALL);
        expected.setGroupByFieldMap(map);
        expected.setSumFields(Sets.newHashSet("AG"));
        expected.setCountFields(Sets.newHashSet("NOME"));
        expected.setAverageFields(Sets.newHashSet("BAR"));
        expected.setMinFields(Sets.newHashSet("BAT"));
        expected.setMaxFields(Sets.newHashSet("FOO"));
        expected.remapFields(inverseReverseModel, reverseModel);

        GroupFields actual = GroupFields.from("GROUP(AG)|SUM(AG)|COUNT(NOME)|AVERAGE(BAR)|MIN(BAT)|MAX(FOO)|REVERSE_MODEL_MAP(AGE=AG:NAME=NOME)");

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test the result of {@link GroupFields#from(String)} given a string with the legacy format of comma-delimited fields only.
     */
    @Test
    public void testFromGivenLegacyFormat() {
        GroupFields expected = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("AGE", TemporalGranularity.ALL);
        map.put("GENDER", TemporalGranularity.ALL);
        map.put("NAME", TemporalGranularity.ALL);
        expected.setGroupByFieldMap(map);

        GroupFields actual = GroupFields.from("AGE,GENDER,NAME");

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test the result of {@link GroupFields#from(String)} given a string with sets of fields with granularities like a user may give as the arguments to the
     * {@code #GROUPBY} function.
     */
    @Test
    public void testFromGivenGroupingFieldsOnlyAndGranularities() {
        GroupFields expected = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("AGE", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        map.put("AGE", TemporalGranularity.TRUNCATE_TEMPORAL_TO_YEAR);
        map.put("GENDER", TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        map.put("NAME", TemporalGranularity.ALL);
        expected.setGroupByFieldMap(map);

        GroupFields actual = GroupFields.from("AGE[DAY,YEAR],GENDER[HOUR],NAME");

        assertThat(actual).isEqualTo(expected);
    }

    /**
     * Test the result of {@link GroupFields#parseGroupByFields(String)} given a null string.
     */
    @Test
    public void testParseGroupByFieldsGivenNull() {
        TreeMultimap<String,TemporalGranularity> actual = GroupFields.parseGroupByFields(null);

        assertTrue(actual.isEmpty());
    }

    /**
     * Test the result of {@link GroupFields#parseGroupByFields(String)} given an empty string.
     */
    @Test
    public void testParseGroupByFieldsGivenEmptyString() {
        TreeMultimap<String,TemporalGranularity> actual = GroupFields.parseGroupByFields("");

        assertTrue(actual.isEmpty());
    }

    /**
     * Test the result of {@link GroupFields#parseGroupByFields(String)} given a string with whitespace.
     */
    @Test
    public void testParseGroupByFieldsGivenWhitespace() {
        TreeMultimap<String,TemporalGranularity> expected = TreeMultimap.create();
        expected.put("AGE", TemporalGranularity.ALL);
        expected.put("GENDER", TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR);

        TreeMultimap<String,TemporalGranularity> actual = GroupFields.parseGroupByFields(" AGE,GENDER[  HOUR]");

        assertEquals(expected, actual);
    }

    /**
     * Test the result of {@link GroupFields#parseGroupByFields(String)} given a string with comma-delimited fields only.
     */
    @Test
    public void testParseGroupByFieldsGivenLegacyFormat() {
        TreeMultimap<String,TemporalGranularity> expected = TreeMultimap.create();
        expected.put("AGE", TemporalGranularity.ALL);
        expected.put("GENDER", TemporalGranularity.ALL);
        expected.put("NAME", TemporalGranularity.ALL);

        TreeMultimap<String,TemporalGranularity> actual = GroupFields.parseGroupByFields("AGE,GENDER,NAME");

        assertEquals(expected, actual);
    }

    /**
     * Test the result of {@link GroupFields#parseGroupByFields(String)} given a string with multiple granularities
     */
    @Test
    public void testParseGroupByFieldsGivenMultipleGranularities() {
        TreeMultimap<String,TemporalGranularity> expected = TreeMultimap.create();
        expected.put("AGE", TemporalGranularity.ALL);
        expected.put("GENDER", TemporalGranularity.TRUNCATE_TEMPORAL_TO_HOUR);
        expected.put("NAME", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        expected.put("NAME", TemporalGranularity.TRUNCATE_TEMPORAL_TO_YEAR);

        TreeMultimap<String,TemporalGranularity> actual = GroupFields.parseGroupByFields("AGE,GENDER[HOUR],NAME[DAY,YEAR]");

        assertEquals(expected, actual);
    }

    @Test
    public void testDeconstructIdentifiers() {
        GroupFields groupFields = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("$AGE", TemporalGranularity.ALL);
        map.put("$GENDER", TemporalGranularity.ALL);
        groupFields.setGroupByFieldMap(map);
        groupFields.setSumFields(Sets.newHashSet("$AGE", "$GENDER"));
        groupFields.setCountFields(Sets.newHashSet("$AGE", "$GENDER"));
        groupFields.setAverageFields(Sets.newHashSet("$AGE", "$GENDER"));
        groupFields.setMinFields(Sets.newHashSet("$AGE", "$GENDER"));
        groupFields.setMaxFields(Sets.newHashSet("$AGE", "$GENDER"));

        groupFields.deconstructIdentifiers();

        assertThat(groupFields.getGroupByFields()).containsExactlyInAnyOrder("AGE", "GENDER");
        assertThat(groupFields.getSumFields()).containsExactlyInAnyOrder("AGE", "GENDER");
        assertThat(groupFields.getCountFields()).containsExactlyInAnyOrder("AGE", "GENDER");
        assertThat(groupFields.getMinFields()).containsExactlyInAnyOrder("AGE", "GENDER");
        assertThat(groupFields.getMaxFields()).containsExactlyInAnyOrder("AGE", "GENDER");
        assertThat(groupFields.getAverageFields()).containsExactlyInAnyOrder("AGE", "GENDER");
    }

    @Test
    public void testRemapFields() {
        GroupFields groupFields = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("AG", TemporalGranularity.ALL);
        map.put("GEN", TemporalGranularity.ALL);
        groupFields.setGroupByFieldMap(map);
        groupFields.setSumFields(Sets.newHashSet("AG"));
        groupFields.setCountFields(Sets.newHashSet("NOME"));
        groupFields.setAverageFields(Sets.newHashSet("AG"));
        groupFields.setMinFields(Sets.newHashSet("GEN"));
        groupFields.setMaxFields(Sets.newHashSet("NOME"));

        groupFields.remapFields(inverseReverseModel, reverseModel);

        assertThat(groupFields.getGroupByFields()).containsExactlyInAnyOrder("GEN", "AG");
        assertThat(groupFields.getSumFields()).containsExactlyInAnyOrder("AG");
        assertThat(groupFields.getCountFields()).containsExactlyInAnyOrder("NOME");
        assertThat(groupFields.getAverageFields()).containsExactlyInAnyOrder("AG");
        assertThat(groupFields.getMinFields()).containsExactlyInAnyOrder("GEN");
        assertThat(groupFields.getMaxFields()).containsExactlyInAnyOrder("NOME");
        assertThat(groupFields.getReverseModelMap()).containsEntry("AGE", "AG").containsEntry("GENDER", "GEN").containsEntry("GENERE", "GEN")
                        .containsEntry("NAME", "NOME").hasSize(4);
    }

    @Test
    public void testSerialization() throws JsonProcessingException {
        GroupFields groupFields = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("AG", TemporalGranularity.ALL);
        map.put("GEN", TemporalGranularity.TRUNCATE_TEMPORAL_TO_DAY);
        groupFields.setGroupByFieldMap(map);
        groupFields.setSumFields(Sets.newHashSet("AG"));
        groupFields.setCountFields(Sets.newHashSet("NOME"));
        groupFields.setAverageFields(Sets.newHashSet("AG"));
        groupFields.setMinFields(Sets.newHashSet("GEN"));
        groupFields.setMaxFields(Sets.newHashSet("NOME"));

        groupFields.remapFields(inverseReverseModel, reverseModel);

        String json = objectMapper.writeValueAsString(groupFields);
        assertThat(json).isEqualTo(
                        "\"GROUP(AG[ALL],GEN[DAY])|SUM(AG)|COUNT(NOME)|AVERAGE(AG)|MIN(GEN)|MAX(NOME)|REVERSE_MODEL_MAP(GENERE=GEN:GENDER=GEN:AGE=AG:NAME=NOME)\"");
    }

    @Test
    public void testDeserialization() throws JsonProcessingException {
        GroupFields expected = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("AG", TemporalGranularity.ALL);
        map.put("GEN", TemporalGranularity.ALL);
        expected.setGroupByFieldMap(map);
        expected.setSumFields(Sets.newHashSet("AG"));
        expected.setCountFields(Sets.newHashSet("NOME"));
        expected.setAverageFields(Sets.newHashSet("AG"));
        expected.setMinFields(Sets.newHashSet("GEN"));
        expected.setMaxFields(Sets.newHashSet("NOME"));
        expected.remapFields(inverseReverseModel, reverseModel);

        String json = "\"GROUP(GEN[ALL],AG[ALL])|SUM(AG)|COUNT(NOME)|AVERAGE(AG)|MIN(GEN)|MAX(NOME)|REVERSE_MODEL_MAP(GENERE=GEN:GENDER=GEN:AGE=AG:NAME=NOME)\"";
        GroupFields actual = objectMapper.readValue(json, GroupFields.class);

        assertThat(actual).isEqualTo(expected);
    }

    @Test
    public void testGetFieldAggregatorFactory() {
        GroupFields groupFields = new GroupFields();
        Multimap<String,TemporalGranularity> map = HashMultimap.create();
        map.put("AGE", TemporalGranularity.ALL);
        map.put("GENDER", TemporalGranularity.ALL);
        groupFields.setGroupByFieldMap(map);
        groupFields.setSumFields(Sets.newHashSet("AGE"));
        groupFields.setCountFields(Sets.newHashSet("NAME"));
        groupFields.setAverageFields(Sets.newHashSet("HEIGHT"));
        groupFields.setMinFields(Sets.newHashSet("SALARY"));
        groupFields.setMaxFields(Sets.newHashSet("RANK"));

        // @formatter:off
        FieldAggregator.Factory expected = new FieldAggregator.Factory().withSumFields("AGE")
                        .withCountFields("NAME")
                        .withAverageFields("HEIGHT")
                        .withMinFields("SALARY")
                        .withMaxFields("RANK");
        // @formatter:on

        assertThat(groupFields.getFieldAggregatorFactory()).isEqualTo(expected);
    }
}
