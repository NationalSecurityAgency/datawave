package datawave.query.common.grouping;

import java.math.BigDecimal;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.assertj.core.util.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;

import datawave.data.type.LcNoDiacriticsType;
import datawave.data.type.NumberType;
import datawave.data.type.Type;
import datawave.query.attributes.Attribute;
import datawave.query.attributes.Attributes;
import datawave.query.attributes.Document;
import datawave.query.attributes.TypeAttribute;
import datawave.test.GroupsAssert;

public class DocumentGrouperTest {

    private static final ColumnVisibility COLVIS_ALL = new ColumnVisibility("ALL");
    private static final ColumnVisibility COLVIS_E = new ColumnVisibility("E");
    private static final ColumnVisibility COLVIS_I = new ColumnVisibility("I");
    private static final ColumnVisibility COLVIS_ALL_E_I = new ColumnVisibility("ALL&E&I");
    private static final Key key = new Key("test_key");
    private static final Multimap<String,String> inverseReverseMap = HashMultimap.create();
    private static final Map<String,String> reverseMap = new HashMap<>();

    private GroupFields groupFields = new GroupFields();
    private Document document;
    private Groups groups;

    @BeforeClass
    public static void beforeClass() {
        inverseReverseMap.put("GEN", "GENERE");
        inverseReverseMap.put("GEN", "GENDER");
        inverseReverseMap.put("AG", "AGE");
        inverseReverseMap.put("AG", "ETA");
        inverseReverseMap.put("LOC", "BUILDING");
        inverseReverseMap.put("LOC", "LOCATION");
        inverseReverseMap.put("PEAK", "HEIGHT");

        reverseMap.put("GENERE", "GEN");
        reverseMap.put("GENDER", "GEN");
        reverseMap.put("AGE", "AG");
        reverseMap.put("ETA", "AG");
        reverseMap.put("BUILDING", "LOC");
        reverseMap.put("LOCATION", "LOC");
        reverseMap.put("HEIGHT", "PEAK");
    }

    @Before
    public void setUp() throws Exception {
        groups = new Groups();
        document = new Document();
        groupFields = new GroupFields();
    }

    /**
     * Verify that when grouping by a single field that has a grouping context and instance, e.g. the format GENDER.FOO.1, that the count is correctly
     * established for each distinct value of the field.
     */
    @Test
    public void testGroupingBySingleFieldWithGroupAndInstance() {
        givenGroupFields("GENDER");

        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.3").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.BAR.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.BAR.2").withLcNoDiacritics("FEMALE"));

        executeGrouping();

        // We should have the following groupings:
        // MALE (Count of 2)
        // FEMALE (Count of 3)
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(2);
        groupsAssert.assertGroup(textKey("GENDER", "MALE")).hasCount(2);
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE")).hasCount(3);
    }

    /**
     * Verify that when grouping by multiple fields, where all entries have a grouping context and instance, and only direct matches need to be grouped, that
     * the count is correct for each grouping. Additionally, verify that the grouping context and instance are parsed correctly from the field names.
     */
    @Test
    public void testGroupingFieldsWithMatchingGroupsAndInstancesAndDirectMatches() {
        givenGroupFields("AGE", "GENDER");

        givenDocumentEntry(DocumentEntry.of("AGE.FOO.A.B.C.1").withNumberType("24"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.A.B.2").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.C.3").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.BAR.B.C.1").withNumberType("40"));
        givenDocumentEntry(DocumentEntry.of("AGE.BAR.V.A.2").withNumberType("20"));

        // Direct match to AGE.FOO.1.
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.A.C.1").withLcNoDiacritics("MALE"));
        // Direct match to AGE.FOO.2
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.V.S.2").withLcNoDiacritics("FEMALE"));
        // Direct match to AGE.FOO.3
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.F.3").withLcNoDiacritics("FEMALE"));
        // No direct match with an AGE record, should be ignored since we have a direct match for a GENDER entry elsewhere.
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.F.G.4").withLcNoDiacritics("FEMALE"));
        // Direct match to AGE.BAR.1.
        givenDocumentEntry(DocumentEntry.of("GENDER.BAR.V.C.A.1").withLcNoDiacritics("MALE"));
        // Direct match to AGE.BAR.2.
        givenDocumentEntry(DocumentEntry.of("GENDER.BAR.G.S.2").withLcNoDiacritics("FEMALE"));

        executeGrouping();

        // We should end up with the following groupings:
        // 24-MALE (Count of 1)
        // 20-FEMALE (Count of 3)
        // 40-MALE (Count of 1)
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(3);
        groupsAssert.assertGroup(textKey("GENDER", "MALE"), numericKey("AGE", "24")).hasCount(1);
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"), numericKey("AGE", "20")).hasCount(3);
        groupsAssert.assertGroup(textKey("GENDER", "MALE"), numericKey("AGE", "40")).hasCount(1);
    }

    /**
     * Verify that when grouping by multiple fields with grouping contexts and instances, but no direct matches, that groupings are created that consist of each
     * field being intersected with each other.
     */
    @Test
    public void testGroupingFieldsWithMatchingGroupsAndInstances() {
        givenGroupFields("AGE", "GENDER", "BUILDING");

        givenDocumentEntry(DocumentEntry.of("AGE.FOO.1").withNumberType("24"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.2").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.3").withNumberType("20"));

        // Direct match to AGE.FOO.1.
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        // Direct match to AGE.FOO.2
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        // Direct match to AGE.FOO.3
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.3").withLcNoDiacritics("FEMALE"));
        // No direct match with an AGE record, should be ignored since we have a direct match for a GENDER entry elsewhere.
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.4").withLcNoDiacritics("FEMALE"));
        // No direct match, but we should have a cartesian product with the AGE-GENDER direct matches.
        givenDocumentEntry(DocumentEntry.of("BUILDING.BAR.1").withLcNoDiacritics("West"));
        // No direct match, but we should have a cartesian product with the AGE-GENDER direct matches.
        givenDocumentEntry(DocumentEntry.of("BUILDING.BAR.2").withLcNoDiacritics("East"));

        executeGrouping();

        // We should end up with the following groupings:
        // 24-MALE-West (Count of 1)
        // 24-MALE-East (Count of 1)
        // 20-FEMALE-West (Count of 2)
        // 20-FEMALE-East (Count of 2)
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(4);
        groupsAssert.assertGroup(textKey("GENDER", "MALE"), numericKey("AGE", "24"), textKey("BUILDING", "West")).hasCount(1);
        groupsAssert.assertGroup(textKey("GENDER", "MALE"), numericKey("AGE", "24"), textKey("BUILDING", "East")).hasCount(1);
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"), numericKey("AGE", "20"), textKey("BUILDING", "West")).hasCount(2);
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"), numericKey("AGE", "20"), textKey("BUILDING", "East")).hasCount(2);
    }

    @Test
    public void testGroupingFieldsWithMatchingGroupsAndInstancesAndMultipleIntersectionalityPoints() {
        givenGroupFields("AGE", "GENDER", "BUILDING", "RECORD_ID", "RECORD_TITLE");

        givenDocumentEntry(DocumentEntry.of("AGE.FOO.1").withNumberType("24"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.2").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.3").withNumberType("20"));

        // Direct match to AGE.FOO.1.
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        // Direct match to AGE.FOO.2.
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        // Direct match to AGE.FOO.3.
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.3").withLcNoDiacritics("FEMALE"));
        // No direct match with an AGE record, should be ignored since we have a direct match for a GENDER entry elsewhere.
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.4").withLcNoDiacritics("FEMALE"));

        givenDocumentEntry(DocumentEntry.of("RECORD_ID.HAT.1").withNumberType("123"));
        givenDocumentEntry(DocumentEntry.of("RECORD_ID.HAT.2").withNumberType("456"));

        // Direct match to RECORD_ID.HAT.1.
        givenDocumentEntry(DocumentEntry.of("RECORD_TITLE.HAT.1").withLcNoDiacritics("Manual"));
        // Direct match to RECORD_ID.HAT.2.
        givenDocumentEntry(DocumentEntry.of("RECORD_TITLE.HAT.2").withLcNoDiacritics("Summary"));

        // No direct match, but we should have a cartesian product with the AGE-GENDER and RECORD_ID-RECORD_TITLE direct matches.
        givenDocumentEntry(DocumentEntry.of("BUILDING.BAR.1").withLcNoDiacritics("West"));
        // No direct match, but we should have a cartesian product with the AGE-GENDER and RECORD_ID-RECORD_TITLE direct matches.
        givenDocumentEntry(DocumentEntry.of("BUILDING.BAR.2").withLcNoDiacritics("East"));

        executeGrouping();

        // We should end up with the following groupings:
        // 24-MALE-123-Manual-West (Count of 1)
        // 24-MALE-456-Summary-West (Count of 1)
        // 24-MALE-123-Manual-East (Count of 1)
        // 24-MALE-456-Summary-East (Count of 1)
        // 20-FEMALE-123-Manual-West (Count of 2)
        // 20-FEMALE-456-Summary-West (Count of 2)
        // 20-FEMALE-123-Manual-East (Count of 2)
        // 20-FEMALE-456-Summary-East (Count of 2)

        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(8);

        // @formatter:off
        groupsAssert.assertGroup(textKey("GENDER", "MALE"),
                        numericKey("AGE", "24"),
                        textKey("BUILDING", "West"),
                        numericKey("RECORD_ID", "123"),
                        textKey("RECORD_TITLE", "Manual")).hasCount(1);

        groupsAssert.assertGroup(textKey("GENDER", "MALE"),
                        numericKey("AGE", "24"),
                        textKey("BUILDING", "West"),
                        numericKey("RECORD_ID", "456"),
                        textKey("RECORD_TITLE", "Summary")).hasCount(1);

        groupsAssert.assertGroup(textKey("GENDER", "MALE"),
                        numericKey("AGE", "24"),
                        textKey("BUILDING", "East"),
                        numericKey("RECORD_ID", "123"),
                        textKey("RECORD_TITLE", "Manual")).hasCount(1);

        groupsAssert.assertGroup(textKey("GENDER", "MALE"),
                        numericKey("AGE", "24"),
                        textKey("BUILDING", "East"),
                        numericKey("RECORD_ID", "456"),
                        textKey("RECORD_TITLE", "Summary")).hasCount(1);

        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"),
                        numericKey("AGE", "20"),
                        textKey("BUILDING", "West"),
                        numericKey("RECORD_ID", "123"),
                        textKey("RECORD_TITLE", "Manual")).hasCount(2);

        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"),
                        numericKey("AGE", "20"),
                        textKey("BUILDING", "West"),
                        numericKey("RECORD_ID", "456"),
                        textKey("RECORD_TITLE", "Summary")).hasCount(2);

        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"),
                        numericKey("AGE", "20"),
                        textKey("BUILDING", "East"),
                        numericKey("RECORD_ID", "123"),
                        textKey("RECORD_TITLE", "Manual")).hasCount(2);

        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"),
                        numericKey("AGE", "20"),
                        textKey("BUILDING", "East"),
                        numericKey("RECORD_ID", "456"),
                        textKey("RECORD_TITLE", "Summary")).hasCount(2);
        // @formatter:on
    }

    @Test
    public void testGroupingBySingleFieldWithInstanceOnly() {
        givenGroupFields("GENDER");

        givenDocumentEntry(DocumentEntry.of("GENDER.1").withLcNoDiacritics("MALE").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.2").withLcNoDiacritics("MALE").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.3").withLcNoDiacritics("FEMALE"));

        executeGrouping();

        // We should have the following groupings:
        // MALE (Count of 2)
        // FEMALE (Count of 3)
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(2);
        groupsAssert.assertGroup(textKey("GENDER", "MALE")).hasCount(2);
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE")).hasCount(3);
    }

    @Test
    public void testGroupingMultipleFieldsWithInstanceOnly() {
        givenGroupFields("BUILDING", "AGE");

        givenDocumentEntry(DocumentEntry.of("BUILDING.1").withLcNoDiacritics("West").withLcNoDiacritics("East"));
        givenDocumentEntry(DocumentEntry.of("BUILDING.2").withLcNoDiacritics("West"));

        // No direct match, we should have a cartesian product with each BUILDING value.
        givenDocumentEntry(DocumentEntry.of("AGE.1").withNumberType("20"));
        // No direct match, we should have a cartesian product with each BUILDING value.
        givenDocumentEntry(DocumentEntry.of("AGE.2").withNumberType("24"));

        executeGrouping();

        // We should have the following groupings:
        // West-20 (Count of 2)
        // West-24 (Count of 2)
        // East-20 (Count of 1)
        // East-24 (Count of 1)
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(4);
        groupsAssert.assertGroup(textKey("BUILDING", "West"), numericKey("AGE", "20")).hasCount(2);
        groupsAssert.assertGroup(textKey("BUILDING", "West"), numericKey("AGE", "24")).hasCount(2);
        groupsAssert.assertGroup(textKey("BUILDING", "East"), numericKey("AGE", "20")).hasCount(1);
        groupsAssert.assertGroup(textKey("BUILDING", "East"), numericKey("AGE", "24")).hasCount(1);
    }

    @Test
    public void testGroupingBySingleFieldWithoutInstance() {
        givenGroupFields("GENDER");

        givenDocumentEntry(DocumentEntry.of("GENDER").withLcNoDiacritics("MALE").withLcNoDiacritics("FEMALE"));

        executeGrouping();

        // We should have the following groupings:
        // MALE (Count of 1)
        // FEMALE (Count of 1)
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(2);
        groupsAssert.assertGroup(textKey("GENDER", "MALE")).hasCount(1);
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE")).hasCount(1);
    }

    @Test
    public void testGroupingByMultipleFieldsWithoutInstance() {
        givenGroupFields("GENDER", "BUILDING");

        givenDocumentEntry(DocumentEntry.of("GENDER").withLcNoDiacritics("MALE").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("BUILDING").withLcNoDiacritics("East").withLcNoDiacritics("West").withLcNoDiacritics("North"));

        executeGrouping();

        // We should have the following groupings:
        // MALE-East (Count of 1)
        // MALE-West (Count of 1)
        // MALE-North (Count of 1)
        // FEMALE-East (Count of 1)
        // FEMALE-West (Count of 1)
        // FEMALE-North (Count of 1)
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(6);
        groupsAssert.assertGroup(textKey("GENDER", "MALE"), textKey("BUILDING", "West")).hasCount(1);
        groupsAssert.assertGroup(textKey("GENDER", "MALE"), textKey("BUILDING", "East")).hasCount(1);
        groupsAssert.assertGroup(textKey("GENDER", "MALE"), textKey("BUILDING", "North")).hasCount(1);
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"), textKey("BUILDING", "West")).hasCount(1);
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"), textKey("BUILDING", "East")).hasCount(1);
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"), textKey("BUILDING", "North")).hasCount(1);
    }

    @Test
    public void testGroupingBySingleFieldAcrossMultipleDocuments() {
        givenGroupFields("GENDER");

        givenDocumentColumnVisibility(COLVIS_ALL);
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE", COLVIS_ALL));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("MALE", COLVIS_ALL));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.3").withLcNoDiacritics("MALE", COLVIS_ALL));

        executeGrouping();

        resetDocument();
        givenDocumentColumnVisibility(COLVIS_E);
        givenDocumentEntry(DocumentEntry.of("GENDER.BAR.1").withLcNoDiacritics("FEMALE", COLVIS_I));
        givenDocumentEntry(DocumentEntry.of("GENDER.BAR.2").withLcNoDiacritics("MALE", COLVIS_I));
        givenDocumentEntry(DocumentEntry.of("GENDER.HAT.1").withLcNoDiacritics("FEMALE", COLVIS_I));

        executeGrouping();

        resetDocument();
        givenDocumentColumnVisibility(COLVIS_I);
        givenDocumentEntry(DocumentEntry.of("GENDER.TIN.1").withLcNoDiacritics("FEMALE", COLVIS_E));

        executeGrouping();

        // We should expect the following groups:
        // MALE (Count of 4)
        // FEMALE (Count of 3)
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(2);

        groupsAssert.assertGroup(textKey("GENDER", "MALE")).hasCount(4).hasDocumentVisibilities(COLVIS_ALL, COLVIS_I)
                        .hasVisibilitiesForKey(textKey("GENDER", "MALE"), COLVIS_ALL, COLVIS_I);
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE")).hasCount(3).hasDocumentVisibilities(COLVIS_I, COLVIS_E)
                        .hasVisibilitiesForKey(textKey("GENDER", "FEMALE"), COLVIS_I, COLVIS_E);
    }

    @Test
    public void testGroupingByMultipleFieldsWithDifferentFormatsAcrossMultipleDocuments() {
        // @formatter:off
        givenGroupFields("GENDER", "BUILDING");

        givenDocumentColumnVisibility(COLVIS_I);
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE", COLVIS_ALL));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE", COLVIS_ALL));
        givenDocumentEntry(DocumentEntry.of("BUILDING.1").withLcNoDiacritics("West", COLVIS_E).withLcNoDiacritics("East", COLVIS_I));

        executeGrouping();

        // This document contains only BUILDING entries. Because of this, we should see a single "North" and "South" grouping.
        resetDocument();
        givenDocumentColumnVisibility(COLVIS_ALL);
        givenDocumentEntry(DocumentEntry.of("BUILDING.1").withLcNoDiacritics("North")
                        .withLcNoDiacritics("South"));

        executeGrouping();

        resetDocument();
        givenDocumentColumnVisibility(COLVIS_E);
        givenDocumentEntry(DocumentEntry.of("GENDER.TIN.1").withLcNoDiacritics("MALE", COLVIS_ALL));
        givenDocumentEntry(DocumentEntry.of("GENDER.TIN.2").withLcNoDiacritics("MALE", COLVIS_ALL));
        givenDocumentEntry(DocumentEntry.of("BUILDING.1").withLcNoDiacritics("Center", COLVIS_ALL));

        executeGrouping();

        // @formatter:on

        // We should expect the following groups:
        // MALE-West (Count of 1)
        // MALE-East (Count of 1)
        // FEMALE-West (Count of 1)
        // FEMALE-East (Count of 1)
        // MALE-Center (Count of 2)
        // North (Count of 1)
        // South (Count of 1)

        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(7);

        groupsAssert.assertGroup(textKey("GENDER", "MALE"), textKey("BUILDING", "West")).hasCount(1).hasDocumentVisibilities(COLVIS_ALL_E_I)
                        .hasVisibilitiesForKey(textKey("GENDER", "MALE"), COLVIS_ALL).hasVisibilitiesForKey(textKey("BUILDING", "West"), COLVIS_E);
        groupsAssert.assertGroup(textKey("GENDER", "MALE"), textKey("BUILDING", "East")).hasCount(1).hasDocumentVisibilities(COLVIS_ALL_E_I)
                        .hasVisibilitiesForKey(textKey("GENDER", "MALE"), COLVIS_ALL).hasVisibilitiesForKey(textKey("BUILDING", "East"), COLVIS_I);
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"), textKey("BUILDING", "West")).hasCount(1).hasDocumentVisibilities(COLVIS_ALL_E_I)
                        .hasVisibilitiesForKey(textKey("GENDER", "FEMALE"), COLVIS_ALL).hasVisibilitiesForKey(textKey("BUILDING", "West"), COLVIS_E);
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"), textKey("BUILDING", "East")).hasCount(1).hasDocumentVisibilities(COLVIS_ALL_E_I)
                        .hasVisibilitiesForKey(textKey("GENDER", "FEMALE"), COLVIS_ALL).hasVisibilitiesForKey(textKey("BUILDING", "East"), COLVIS_I);
        groupsAssert.assertGroup(textKey("GENDER", "MALE"), textKey("BUILDING", "Center")).hasCount(2).hasDocumentVisibilities(COLVIS_ALL)
                        .hasVisibilitiesForKey(textKey("GENDER", "MALE"), COLVIS_ALL).hasVisibilitiesForKey(textKey("BUILDING", "Center"), COLVIS_ALL);
        groupsAssert.assertGroup(textKey("BUILDING", "North")).hasCount(1).hasDocumentVisibilities(COLVIS_ALL);
        groupsAssert.assertGroup(textKey("BUILDING", "South")).hasCount(1).hasDocumentVisibilities(COLVIS_ALL);
    }

    @Test
    public void testAggregatingFieldWithGroupingContextAndInstanceWithDirectMatches() {
        givenGroupFields("GENDER");
        givenSumFields("AGE");
        givenMaxFields("AGE");
        givenMinFields("AGE");
        givenCountFields("AGE");
        givenAverageFields("AGE");

        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.3").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.4").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.5").withLcNoDiacritics("FEMALE"));

        // Should aggregate to any grouping that contains GENDER.FOO.1.
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.1").withNumberType("20"));
        // Should aggregate to any grouping that contains GENDER.FOO.2.
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.2").withNumberType("5"));
        // Should aggregate to any grouping that contains GENDER.FOO.3.
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.3").withNumberType("15"));
        // Should aggregate to any grouping that contains GENDER.FOO.4.
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.4").withNumberType("30"));
        // Should aggregate to any grouping that contains GENDER.FOO.5.
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.5").withNumberType("50"));
        // Should not aggregate to any groupings since it does not have a direct match, but other direct matches exist for AGE.
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.6").withNumberType("100"));

        executeGrouping();

        // We should expect the following groups:
        // MALE (Count of 2)
        // FEMALE (Count of 3)
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(2);

        // @formatter:off
        groupsAssert.assertGroup(textKey("GENDER", "MALE")).hasCount(2)
                        .hasAggregatedSum("AGE", new BigDecimal("35"))
                        .hasAggregatedCount("AGE", 2L)
                        .hasAggregatedAverage("AGE", new BigDecimal("17.5"))
                        .hasAggregatedMax("AGE", new NumberType("20"))
                        .hasAggregatedMin("AGE", new NumberType("15"));

        groupsAssert.assertGroup(textKey("GENDER", "FEMALE")).hasCount(3)
                        .hasAggregatedSum("AGE", new BigDecimal("85"))
                        .hasAggregatedCount("AGE", 3L)
                        .hasAggregatedAverage("AGE", new BigDecimal("28.33333333"))
                        .hasAggregatedMax("AGE", new NumberType("50"))
                        .hasAggregatedMin("AGE", new NumberType("5"));
        // @formatter:on
    }

    @Test
    public void testAggregatingFieldWithGroupingContextAndInstanceWithNoDirectMatches() {
        givenGroupFields("GENDER");
        givenSumFields("AGE");
        givenMaxFields("AGE");
        givenMinFields("AGE");
        givenCountFields("AGE");
        givenAverageFields("AGE");

        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.3").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.4").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.5").withLcNoDiacritics("FEMALE"));

        // Every AGE value should aggregate to every GENDER grouping since no direct matches exist between an AGE and GENDER entry.
        givenDocumentEntry(DocumentEntry.of("AGE.BAR.1").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.BAR.2").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("AGE.BAR.3").withNumberType("15"));
        givenDocumentEntry(DocumentEntry.of("AGE.HAT.1").withNumberType("30"));
        givenDocumentEntry(DocumentEntry.of("AGE.HAT.2").withNumberType("50"));

        executeGrouping();

        // We should expect the following groups:
        // MALE (Count of 2)
        // FEMALE (Count of 3)
        // We should also expect the aggregation results to be the same for each group.
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(2);

        // @formatter:off
        groupsAssert.assertGroup(textKey("GENDER", "MALE")).hasCount(2)
                        .hasAggregatedSum("AGE", new BigDecimal("120"))
                        .hasAggregatedCount("AGE", 5L)
                        .hasAggregatedAverage("AGE", new BigDecimal("24"))
                        .hasAggregatedMax("AGE", new NumberType("50"))
                        .hasAggregatedMin("AGE", new NumberType("5"));

        groupsAssert.assertGroup(textKey("GENDER", "FEMALE")).hasCount(3)
                        .hasAggregatedSum("AGE", new BigDecimal("120"))
                        .hasAggregatedCount("AGE", 5L)
                        .hasAggregatedAverage("AGE", new BigDecimal("24"))
                        .hasAggregatedMax("AGE", new NumberType("50"))
                        .hasAggregatedMin("AGE", new NumberType("5"));
        // @formatter:on
    }

    @Test
    public void testAggregatingFieldWithInstanceOnly() {
        givenGroupFields("GENDER");
        givenSumFields("AGE");
        givenMaxFields("AGE");
        givenMinFields("AGE");
        givenCountFields("AGE");
        givenAverageFields("AGE");

        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.3").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.4").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.5").withLcNoDiacritics("FEMALE"));

        // Every AGE value should aggregate to every GENDER grouping since no direct matches exist between an AGE and GENDER entry.
        givenDocumentEntry(DocumentEntry.of("AGE.1").withNumberType("20").withNumberType("5").withNumberType("15"));
        givenDocumentEntry(DocumentEntry.of("AGE.2").withNumberType("30").withNumberType("50"));

        executeGrouping();

        // We should expect the following groups:
        // MALE (Count of 2)
        // FEMALE (Count of 3)
        // We should also expect the aggregation results to be the same for each group.
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(2);

        // @formatter:off
        groupsAssert.assertGroup(textKey("GENDER", "MALE")).hasCount(2)
                        .hasAggregatedSum("AGE", new BigDecimal("120"))
                        .hasAggregatedCount("AGE", 5L)
                        .hasAggregatedAverage("AGE", new BigDecimal("24"))
                        .hasAggregatedMax("AGE", new NumberType("50"))
                        .hasAggregatedMin("AGE", new NumberType("5"));

        groupsAssert.assertGroup(textKey("GENDER", "FEMALE")).hasCount(3)
                        .hasAggregatedSum("AGE", new BigDecimal("120"))
                        .hasAggregatedCount("AGE", 5L)
                        .hasAggregatedAverage("AGE", new BigDecimal("24"))
                        .hasAggregatedMax("AGE", new NumberType("50"))
                        .hasAggregatedMin("AGE", new NumberType("5"));
        // @formatter:on
    }

    @Test
    public void testAggregatingFieldWithoutInstance() {
        givenGroupFields("GENDER");
        givenSumFields("AGE");
        givenMaxFields("AGE");
        givenMinFields("AGE");
        givenCountFields("AGE");
        givenAverageFields("AGE");

        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.3").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.4").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.5").withLcNoDiacritics("FEMALE"));

        // Every AGE value should aggregate to every GENDER grouping since no direct matches exist between an AGE and GENDER entry.
        givenDocumentEntry(DocumentEntry.of("AGE").withNumberType("20").withNumberType("5").withNumberType("15").withNumberType("30").withNumberType("50"));

        executeGrouping();

        // We should expect the following groups:
        // MALE (Count of 2)
        // FEMALE (Count of 3)
        // We should also expect the aggregation results to be the same for each group.
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(2);

        // @formatter:off
        groupsAssert.assertGroup(textKey("GENDER", "MALE")).hasCount(2)
                        .hasAggregatedSum("AGE", new BigDecimal("120"))
                        .hasAggregatedCount("AGE", 5L)
                        .hasAggregatedAverage("AGE", new BigDecimal("24"))
                        .hasAggregatedMax("AGE", new NumberType("50"))
                        .hasAggregatedMin("AGE", new NumberType("5"));

        groupsAssert.assertGroup(textKey("GENDER", "FEMALE")).hasCount(3)
                        .hasAggregatedSum("AGE", new BigDecimal("120"))
                        .hasAggregatedCount("AGE", 5L)
                        .hasAggregatedAverage("AGE", new BigDecimal("24"))
                        .hasAggregatedMax("AGE", new NumberType("50"))
                        .hasAggregatedMin("AGE", new NumberType("5"));
        // @formatter:on
    }

    @Test
    public void testAggregatingFieldsWithMixedFormats() {
        givenGroupFields("GENDER", "AGE");
        givenSumFields("HEIGHT");
        givenMaxFields("HEIGHT", "BUILDING");
        givenMinFields("BUILDING");
        givenCountFields("HEIGHT", "BUILDING");
        givenAverageFields("HEIGHT");

        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.3").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.4").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.5").withLcNoDiacritics("FEMALE"));

        givenDocumentEntry(DocumentEntry.of("AGE.FOO.1").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.2").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.3").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.4").withNumberType("30"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.5").withNumberType("5"));

        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.1").withNumberType("50"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.2").withNumberType("65"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.3").withNumberType("60"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.4").withNumberType("55"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.5").withNumberType("48"));

        givenDocumentEntry(DocumentEntry.of("BUILDING.1").withLcNoDiacritics("West"));
        givenDocumentEntry(DocumentEntry.of("BUILDING.2").withLcNoDiacritics("North").withLcNoDiacritics("East"));

        executeGrouping();

        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(3);

        // @formatter:off
        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"), numericKey("AGE", "30")).hasCount(1)
                        .hasAggregatedMax("BUILDING", new LcNoDiacriticsType("West"))
                        .hasAggregatedMin("BUILDING", new LcNoDiacriticsType("East"))
                        .hasAggregatedCount("BUILDING", 3L)
                        .hasAggregatedMax("HEIGHT", new NumberType("55"))
                        .hasAggregatedCount("HEIGHT", 1L)
                        .hasAggregatedSum("HEIGHT", new BigDecimal("55"))
                        .hasAggregatedAverage("HEIGHT", new BigDecimal("55"));

        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"), numericKey("AGE", "5")).hasCount(2)
                        .hasAggregatedMax("BUILDING", new LcNoDiacriticsType("West"))
                        .hasAggregatedMin("BUILDING", new LcNoDiacriticsType("East"))
                        .hasAggregatedCount("BUILDING", 3L)
                        .hasAggregatedMax("HEIGHT", new NumberType("65"))
                        .hasAggregatedCount("HEIGHT", 2L)
                        .hasAggregatedSum("HEIGHT", new BigDecimal("113"))
                        .hasAggregatedAverage("HEIGHT", new BigDecimal("56.5"));

        groupsAssert.assertGroup(textKey("GENDER", "MALE"), numericKey("AGE", "20")).hasCount(2)
                        .hasAggregatedMax("BUILDING", new LcNoDiacriticsType("West"))
                        .hasAggregatedMin("BUILDING", new LcNoDiacriticsType("East"))
                        .hasAggregatedCount("BUILDING", 3L)
                        .hasAggregatedMax("HEIGHT", new NumberType("60"))
                        .hasAggregatedCount("HEIGHT", 2L)
                        .hasAggregatedSum("HEIGHT", new BigDecimal("110"))
                        .hasAggregatedAverage("HEIGHT", new BigDecimal("55"));
        // @formatter:on
    }

    @Test
    public void testAggregationAcrossMultipleDocuments() {
        givenGroupFields("GENDER", "AGE");
        givenSumFields("HEIGHT");

        // We should see groups being counted and aggregation for HEIGHT occurring.
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.1").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.2").withNumberType("15"));
        // Should aggregate to FOO.1 grouping.
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.1").withNumberType("5"));
        // Should aggregate to FOO.2 grouping.
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.2").withNumberType("5"));
        // Should not aggregate to anything since there is no direct match for this HEIGHT entry but there are direct matches for other HEIGHT entries.
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.3").withNumberType("5"));

        executeGrouping();

        // We should see groups being counted, but no aggregation should occur since there are no HEIGHT entries.
        resetDocument();
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.1").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.2").withNumberType("15"));

        executeGrouping();

        // We should see single value groupings for "MALE" and "FEMALE" being count, with aggregation for HEIGHT occurring.
        resetDocument();
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.3").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.4").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.1").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.2").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.3").withNumberType("5"));

        executeGrouping();

        // We should see groups being counted and aggregation for HEIGHT occurring.
        resetDocument();
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.1").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.2").withNumberType("15"));
        // Should aggregate to FOO.1 grouping.
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.1").withNumberType("5"));
        // Should aggregate to FOO.2 grouping.
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.2").withNumberType("5"));

        executeGrouping();

        // We should see the HEIGHT aggregated towards an empty grouping.
        resetDocument();
        givenDocumentEntry(DocumentEntry.of("ADDRESS").withLcNoDiacritics("Los Angeles"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.1").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.2").withNumberType("10"));

        executeGrouping();

        // We should see the empty grouping count increase by 1.
        resetDocument();
        givenDocumentEntry(DocumentEntry.of("ADDRESS").withLcNoDiacritics("New York City"));

        executeGrouping();

        // We should see the empty grouping count increase by 1 and the HEIGHT entries aggregated towards it.
        resetDocument();
        givenDocumentEntry(DocumentEntry.of("ADDRESS").withLcNoDiacritics("San Diego"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.1").withNumberType("1"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.2").withNumberType("1"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.3").withNumberType("1"));

        executeGrouping();

        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(6);

        // @formatter:off
        groupsAssert.assertGroup(textKey("GENDER", "MALE")).hasCount(1)
                        .hasAggregatedSum("HEIGHT", new BigDecimal("5"));

        groupsAssert.assertGroup(textKey("GENDER", "FEMALE")).hasCount(3)
                        .hasAggregatedSum("HEIGHT", new BigDecimal("10"));

        groupsAssert.assertGroup(textKey("GENDER", "MALE"), numericKey("AGE", "15")).hasCount(1)
                        .hasAggregatedSum("HEIGHT", new BigDecimal("5"));

        groupsAssert.assertGroup(textKey("GENDER", "FEMALE"), numericKey("AGE", "15")).hasCount(2)
                        .hasAggregatedSum("HEIGHT", new BigDecimal("5"));

        groupsAssert.assertGroup(textKey("GENDER", "MALE"), numericKey("AGE", "20")).hasCount(3)
                        .hasAggregatedSum("HEIGHT", new BigDecimal("10"));

        groupsAssert.assertGroup(Grouping.emptyGrouping()).hasCount(3)
                        .hasAggregatedSum("HEIGHT", new BigDecimal("18"));
        // @formatter:on
    }

    @Test
    public void testAggregatingFieldsWithMixedFormatsWithModelMapping() {
        givenGroupFields("GEN", "AG");
        givenSumFields("PEAK");
        givenMaxFields("PEAK", "LOC");
        givenMinFields("LOC");
        givenCountFields("PEAK", "LOC");
        givenAverageFields("PEAK");

        givenQueryModelApplied();

        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.3").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.4").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.5").withLcNoDiacritics("FEMALE"));

        givenDocumentEntry(DocumentEntry.of("ETA.FOO.1").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("ETA.FOO.2").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("ETA.FOO.3").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("ETA.FOO.4").withNumberType("30"));
        givenDocumentEntry(DocumentEntry.of("ETA.FOO.5").withNumberType("5"));

        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.1").withNumberType("50"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.2").withNumberType("65"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.3").withNumberType("60"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.4").withNumberType("55"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.5").withNumberType("48"));

        givenDocumentEntry(DocumentEntry.of("LOCATION.1").withLcNoDiacritics("West"));
        givenDocumentEntry(DocumentEntry.of("LOCATION.2").withLcNoDiacritics("North").withLcNoDiacritics("East"));

        executeGrouping();

        // We should see each field mapped to their root model name.
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(3);

        // @formatter:off
        groupsAssert.assertGroup(textKey("GEN", "FEMALE"), numericKey("AG", "30")).hasCount(1)
                        .hasAggregatedMax("LOC", new LcNoDiacriticsType("West"))
                        .hasAggregatedMin("LOC", new LcNoDiacriticsType("East"))
                        .hasAggregatedCount("LOC", 3L)
                        .hasAggregatedMax("PEAK", new NumberType("55"))
                        .hasAggregatedCount("PEAK", 1L)
                        .hasAggregatedSum("PEAK", new BigDecimal("55"))
                        .hasAggregatedAverage("PEAK", new BigDecimal("55"));

        groupsAssert.assertGroup(textKey("GEN", "FEMALE"), numericKey("AG", "5")).hasCount(2)
                        .hasAggregatedMax("LOC", new LcNoDiacriticsType("West"))
                        .hasAggregatedMin("LOC", new LcNoDiacriticsType("East"))
                        .hasAggregatedCount("LOC", 3L)
                        .hasAggregatedMax("PEAK", new NumberType("65"))
                        .hasAggregatedCount("PEAK", 2L)
                        .hasAggregatedSum("PEAK", new BigDecimal("113"))
                        .hasAggregatedAverage("PEAK", new BigDecimal("56.5"));

        groupsAssert.assertGroup(textKey("GEN", "MALE"), numericKey("AG", "20")).hasCount(2)
                        .hasAggregatedMax("LOC", new LcNoDiacriticsType("West"))
                        .hasAggregatedMin("LOC", new LcNoDiacriticsType("East"))
                        .hasAggregatedCount("LOC", 3L)
                        .hasAggregatedMax("PEAK", new NumberType("60"))
                        .hasAggregatedCount("PEAK", 2L)
                        .hasAggregatedSum("PEAK", new BigDecimal("110"))
                        .hasAggregatedAverage("PEAK", new BigDecimal("55"));
        // @formatter:on
    }

    @Test
    public void testAggregationAcrossMultipleDocumentsWithModelMapping() {
        givenGroupFields("GEN", "AG");
        givenSumFields("PEAK");

        givenQueryModelApplied();

        // We should see groups being counted and aggregation for HEIGHT occurring.
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.1").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.2").withNumberType("15"));
        // Should aggregate to FOO.1 grouping.
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.1").withNumberType("5"));
        // Should aggregate to FOO.2 grouping.
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.2").withNumberType("5"));
        // Should not aggregate to anything since there is no direct match for this HEIGHT entry but there are direct matches for other HEIGHT entries.
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.3").withNumberType("5"));

        executeGrouping();

        // We should see the groups "MALE" and "FEMALE" with no aggregation.
        resetDocument();
        givenDocumentEntry(DocumentEntry.of("GENERE.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENERE.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("ETA.FOO.1").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("ETA.FOO.2").withNumberType("15"));

        executeGrouping();

        // We should see the groups "MALE" and "FEMALE" with aggregation of the HEIGHT entries.
        resetDocument();
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.1").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.2").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.3").withNumberType("5"));

        executeGrouping();

        // We should see groups being counted and aggregation for HEIGHT occurring.
        resetDocument();
        givenDocumentEntry(DocumentEntry.of("GEN.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GEN.FOO.2").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("AG.FOO.1").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AG.FOO.2").withNumberType("15"));
        // Should aggregate to FOO.1 grouping.
        givenDocumentEntry(DocumentEntry.of("PEAK.FOO.1").withNumberType("5"));
        // Should aggregate to FOO.2 grouping.
        givenDocumentEntry(DocumentEntry.of("PEAK.FOO.2").withNumberType("5"));

        executeGrouping();

        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(5);

        // We should see each field mapped to the root model mapping name, e.g. GENDER -> GEN, AGE -> AG, HEIGHT -> PEAK, etc.
        // @formatter:off
        groupsAssert.assertGroup(textKey("GEN", "MALE")).hasCount(1)
                        .hasAggregatedSum("PEAK", new BigDecimal("5"));

        groupsAssert.assertGroup(textKey("GEN", "FEMALE")).hasCount(1)
                        .hasAggregatedSum("PEAK", new BigDecimal("5"));

        groupsAssert.assertGroup(textKey("GEN", "MALE"), numericKey("AG", "15")).hasCount(1)
                        .hasAggregatedSum("PEAK", new BigDecimal("5"));

        groupsAssert.assertGroup(textKey("GEN", "FEMALE"), numericKey("AG", "15")).hasCount(2)
                        .hasAggregatedSum("PEAK", new BigDecimal("5"));

        groupsAssert.assertGroup(textKey("GEN", "MALE"), numericKey("AG", "20")).hasCount(3)
                        .hasAggregatedSum("PEAK", new BigDecimal("10"));
        // @formatter:on
    }

    @Test
    public void testAggregationAcrossDocumentsWithNoGroups() {
        givenGroupFields("GEN", "AG");
        givenSumFields("PEAK");

        givenQueryModelApplied();

        // We should see an 'empty' group and aggregation for HEIGHT occurring.
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.1").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.2").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.3").withNumberType("5"));

        executeGrouping();

        // We should see an 'empty' group with no aggregation.
        resetDocument();
        givenDocumentEntry(DocumentEntry.of("ADDRESS").withLcNoDiacritics("Los Angeles").withLcNoDiacritics("San Diego").withLcNoDiacritics("Baltimore"));

        executeGrouping();

        // We should see an 'empty' group and aggregation for HEIGHT occurring.
        resetDocument();
        givenDocumentEntry(DocumentEntry.of("ADDRESS").withLcNoDiacritics("Denver"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.1").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.2").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.3").withNumberType("5"));

        executeGrouping();

        // We should see an 'empty' group and aggregation for HEIGHT occurring.
        resetDocument();
        givenDocumentEntry(DocumentEntry.of("ADDRESS").withLcNoDiacritics("Denver"));
        givenDocumentEntry(DocumentEntry.of("PEAK.FOO.1").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("PEAK.FOO.2").withNumberType("5"));

        executeGrouping();

        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(1);

        // We should see each field mapped to the root model mapping name, e.g. GENDER -> GEN, AGE -> AG, HEIGHT -> PEAK, etc.
        // @formatter:off
        groupsAssert.assertGroup().hasCount(4).hasAggregatedSum("PEAK", new BigDecimal("40"));
        // @formatter:on
    }

    /**
     * Verify that given fields with the same value that have the same model mapping, that only one of each distinct field-value pairing are counted.
     */
    @Test
    public void testDedupingEquivalentEntriesWithModelMapping() {
        givenGroupFields("GEN", "AG");
        givenCountFields("AG");
        givenSumFields("AG");
        givenAverageFields("AG");
        givenMaxFields("AG");
        givenMinFields("AG");

        givenQueryModelApplied();

        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENERE.FOO.1").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENERE.FOO.2").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.3").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENERE.FOO.3").withLcNoDiacritics("MALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.4").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENERE.FOO.4").withLcNoDiacritics("FEMALE"));
        givenDocumentEntry(DocumentEntry.of("GENDER.FOO.5").withLcNoDiacritics("FEMALE"));

        givenDocumentEntry(DocumentEntry.of("ETA.FOO.1").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.1").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("ETA.FOO.2").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("ETA.FOO.3").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.3").withNumberType("20"));
        givenDocumentEntry(DocumentEntry.of("ETA.FOO.4").withNumberType("30"));
        givenDocumentEntry(DocumentEntry.of("ETA.FOO.5").withNumberType("5"));
        givenDocumentEntry(DocumentEntry.of("AGE.FOO.5").withNumberType("5"));

        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.1").withNumberType("50"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.2").withNumberType("65"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.3").withNumberType("60"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.4").withNumberType("55"));
        givenDocumentEntry(DocumentEntry.of("HEIGHT.FOO.5").withNumberType("48"));

        givenDocumentEntry(DocumentEntry.of("LOCATION.1").withLcNoDiacritics("West"));
        givenDocumentEntry(DocumentEntry.of("LOCATION.2").withLcNoDiacritics("North").withLcNoDiacritics("East"));

        executeGrouping();

        // We should see each field mapped to their root model name.
        GroupsAssert groupsAssert = GroupsAssert.assertThat(groups);
        groupsAssert.hasTotalGroups(3);

        // @formatter:off
        groupsAssert.assertGroup(textKey("GEN", "FEMALE"), numericKey("AG", "30")).hasCount(1)
                        .hasAggregatedCount("AG", 1L)
                        .hasAggregatedAverage("AG", new BigDecimal("30"))
                        .hasAggregatedSum("AG", new BigDecimal("30"))
                        .hasAggregatedMin("AG", new NumberType("30"))
                        .hasAggregatedMax("AG", new NumberType("30"));

        groupsAssert.assertGroup(textKey("GEN", "FEMALE"), numericKey("AG", "5")).hasCount(2)
                        .hasAggregatedCount("AG", 2L)
                        .hasAggregatedAverage("AG", new BigDecimal("5"))
                        .hasAggregatedSum("AG", new BigDecimal("10"))
                        .hasAggregatedMin("AG", new NumberType("5"))
                        .hasAggregatedMax("AG", new NumberType("5"));

        groupsAssert.assertGroup(textKey("GEN", "MALE"), numericKey("AG", "20")).hasCount(2)
                        .hasAggregatedCount("AG", 2L)
                        .hasAggregatedAverage("AG", new BigDecimal("20"))
                        .hasAggregatedSum("AG", new BigDecimal("40"))
                        .hasAggregatedMin("AG", new NumberType("20"))
                        .hasAggregatedMax("AG", new NumberType("20"));
        // @formatter:on
    }

    private void givenGroupFields(String... fields) {
        groupFields.setGroupByFields(Sets.newHashSet(Arrays.asList(fields)));
    }

    private void givenSumFields(String... fields) {
        groupFields.setSumFields(Sets.newHashSet(Arrays.asList(fields)));
    }

    private void givenCountFields(String... fields) {
        groupFields.setCountFields(Sets.newHashSet(Arrays.asList(fields)));
    }

    private void givenAverageFields(String... fields) {
        groupFields.setAverageFields(Sets.newHashSet(Arrays.asList(fields)));
    }

    private void givenMinFields(String... fields) {
        groupFields.setMinFields(Sets.newHashSet(Arrays.asList(fields)));
    }

    private void givenMaxFields(String... fields) {
        groupFields.setMaxFields(Sets.newHashSet(Arrays.asList(fields)));
    }

    private void givenQueryModelApplied() {
        this.groupFields.remapFields(inverseReverseMap, reverseMap);
    }

    private void resetDocument() {
        this.document = new Document();
    }

    private void givenDocumentEntry(DocumentEntry builder) {
        builder.addEntryTo(this.document);
    }

    private void givenDocumentColumnVisibility(ColumnVisibility columnVisibility) {
        this.document.setColumnVisibility(columnVisibility);
    }

    private void executeGrouping() {
        Map.Entry<Key,Document> keyDocumentEntry = new AbstractMap.SimpleEntry<>(key, this.document);
        DocumentGrouper.group(keyDocumentEntry, this.groupFields, this.groups);
    }

    private GroupingAttribute<?> numericKey(String key, String value) {
        return createGroupingAttribute(key, new NumberType(value));
    }

    private GroupingAttribute<?> textKey(String key, String value) {
        return createGroupingAttribute(key, new LcNoDiacriticsType(value));
    }

    private GroupingAttribute<?> createGroupingAttribute(String key, Type<?> type) {
        return new GroupingAttribute<>(type, new Key(key), true);
    }

    private static class DocumentEntry {
        private final String fieldName;
        private final List<Attribute<?>> attributes = new ArrayList<>();

        public static DocumentEntry of(String fieldName) {
            return new DocumentEntry(fieldName);
        }

        public DocumentEntry(String fieldName) {
            this.fieldName = fieldName;
        }

        public DocumentEntry withNumberType(String value) {
            return withNumberType(value, COLVIS_ALL);
        }

        public DocumentEntry withNumberType(String value, ColumnVisibility visibility) {
            addTypedAttribute(new NumberType(value), visibility);
            return this;
        }

        public DocumentEntry withLcNoDiacritics(String value) {
            return withLcNoDiacritics(value, COLVIS_ALL);
        }

        public DocumentEntry withLcNoDiacritics(String value, ColumnVisibility visibility) {
            addTypedAttribute(new LcNoDiacriticsType(value), visibility);
            return this;
        }

        private void addTypedAttribute(Type<?> type, ColumnVisibility visibility) {
            TypeAttribute<?> attribute = new TypeAttribute<>(type, new Key("cf", "cq"), true);
            attribute.setColumnVisibility(visibility);
            this.attributes.add(attribute);
        }

        public void addEntryTo(Document document) {
            if (attributes.isEmpty()) {
                throw new IllegalArgumentException("No attributes set for document entry");
            } else if (attributes.size() == 1) {
                document.put(fieldName, this.attributes.get(0), true);
            } else {
                document.put(fieldName, new Attributes(this.attributes, true), true);
            }
        }
    }
}
