package datawave.age.off.util;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.StringWriter;

import org.apache.xerces.dom.DocumentImpl;
import org.junit.Test;
import org.w3c.dom.Element;

public class AgeOffRuleFormatterTest {
    /**
     *
     * <ttl units="d">720</ttl> <datatypes>foo,bar</datatypes> <bar.ttl>44</bar.ttl>
     *
     * @throws IOException
     */
    @Test
    public void createRuleFromCsv() throws IOException {
        // @formatter:off
        String expectedOutputText =
                "<rule label=\"test\" mode=\"merge\">\n" +
                "     <filterClass>datawave.ingest.util.cache.watch.TestTrieFilter</filterClass>\n" +
                "     <matchPattern>\n" +
                "          dryFood bakingPowder=365d\n" +
                "          dryFood driedBeans=548d\n" +
                "          dryFood bakingSoda=720d\n" +
                "          dryFood coffeeGround=90d\n" +
                "          dryFood coffeeWholeBean=183d\n" +
                "          dryFood coffeeInstant=730d\n" +
                "          dryFood twinkies=" + Integer.MAX_VALUE + "d\n" +
                "     </matchPattern>\n" +
                "</rule>\n";
        // @formatter:on

        AgeOffCsvToMatchPatternFormatterConfiguration.Builder patternBuilder = new AgeOffCsvToMatchPatternFormatterConfiguration.Builder();
        patternBuilder.useStaticLabel("dryFood");
        patternBuilder.setInput(AgeOffCsvToMatchPatternFormatterTest.INPUT_TEXT_WITHOUT_LABEL.lines().iterator());

        AgeOffRuleConfiguration.Builder builder = new AgeOffRuleConfiguration.Builder();
        builder.withPatternConfigurationBuilder(patternBuilder);
        builder.withRuleLabel("test");
        builder.withFilterClass(datawave.ingest.util.cache.watch.TestTrieFilter.class);
        builder.useMerge();

        assertEquals(expectedOutputText, generateRule(builder));
    }

    @Test
    public void createRuleWithoutMatchPatternFormatter() throws IOException {
        // @formatter:off
        String expectedOutputText =
                "<rule>\n" +
                "     <filterClass>datawave.ingest.util.cache.watch.TestFilter</filterClass>\n" +
                "     <ttl units=\"ms\">10</ttl>\n" +
                "     <matchPattern>1</matchPattern>\n" +
                "     <myTagName ttl=\"1234\"/>\n" +
                "     <filtersWater>false</filtersWater>\n" +
                "</rule>\n";
        // @formatter:on

        AgeOffRuleConfiguration.Builder builder = new AgeOffRuleConfiguration.Builder();
        builder.withFilterClass(datawave.ingest.util.cache.watch.TestFilter.class);

        String duration = "10";
        String units = "ms";
        builder.withTtl(duration, units);

        builder.addSimpleElement("matchPattern", "1");

        Element element = new DocumentImpl().createElement("myTagName");
        element.setAttribute("ttl", "1234");
        builder.addCustomElement(element);

        builder.addSimpleElement("filtersWater", Boolean.FALSE.toString());

        assertEquals(expectedOutputText, generateRule(builder));
    }

    @Test
    public void createRuleFromDataTypeAgeOffFilterJavaDoc() throws IOException {
        // @formatter:off
        String expectedOutputText =
                "<rule>\n" +
                "     <filterClass>datawave.iterators.filter.ageoff.DataTypeAgeOffFilter</filterClass>\n" +
                "     <ttl units=\"d\">720</ttl>\n" +
                "     <datatypes>foo,bar</datatypes>\n" +
                "     <bar.ttl>44</bar.ttl>\n" +
                "</rule>\n";
        // @formatter:on

        AgeOffRuleConfiguration.Builder builder = new AgeOffRuleConfiguration.Builder();
        builder.withIndentation("\t");
        builder.withFilterClass(datawave.iterators.filter.ageoff.DataTypeAgeOffFilter.class);

        String duration = "720";
        String units = "d";
        builder.withTtl(duration, units);

        builder.addSimpleElement("datatypes", "foo,bar");
        builder.addSimpleElement("bar.ttl", "44");

        assertEquals(expectedOutputText, generateRule(builder));
    }

    @Test
    public void createRuleForDataTypeIndexTable() throws IOException {
        // @formatter:off
        String expectedOutputText =
                "<rule mode=\"merge\">\n" +
                "  <filterClass>datawave.iterators.filter.ageoff.DataTypeAgeOffFilter</filterClass>\n" +
                "  <isindextable>true</isindextable>\n" +
                "</rule>\n";
        // @formatter:on

        AgeOffRuleConfiguration.Builder builder = new AgeOffRuleConfiguration.Builder();
        builder.withIndentation("  ");
        builder.useMerge();
        builder.withFilterClass(datawave.iterators.filter.ageoff.DataTypeAgeOffFilter.class);

        builder.addSimpleElement("isindextable", Boolean.TRUE.toString());

        assertEquals(expectedOutputText, generateRule(builder));
    }

    @Test
    public void createFieldAgeOffRule() throws IOException {
        // @formatter:off
        String fieldAgeOffRule =
                "<rule>\n" +
                "  <filterClass>datawave.iterators.filter.ageoff.FieldAgeOffFilter</filterClass>\n" +
                "  <ttl units=\"s\">5</ttl>\n" +
                "  <isindextable>true</isindextable>\n" +
                "  <fields>field_y,field_z</fields>\n" +
                "  <field_y.ttl>1</field_y.ttl>\n" +
                "  <field_z.ttl>2</field_z.ttl>\n" +
                "</rule>\n";
        // @formatter:on

        AgeOffRuleConfiguration.Builder builder = new AgeOffRuleConfiguration.Builder();
        builder.withIndentation("  ");
        builder.withFilterClass(datawave.iterators.filter.ageoff.FieldAgeOffFilter.class);

        String duration = "5";
        String units = "s";
        builder.withTtl(duration, units);

        builder.addSimpleElement("isindextable", Boolean.TRUE.toString());

        builder.addSimpleElement("fields", "field_y,field_z");

        builder.addSimpleElement("field_y.ttl", "1");
        builder.addSimpleElement("field_z.ttl", "2");

        assertEquals(fieldAgeOffRule, generateRule(builder));
    }

    @Test
    public void createFieldAgeOffRuleTtlAlternative() throws IOException {
        // @formatter:off
        String fieldAgeOffRule =
                "<rule>\n" +
                "  <filterClass>datawave.iterators.filter.ageoff.FieldAgeOffFilter</filterClass>\n" +
                "  <ttlUnits>s</ttlUnits>\n" +
                "  <ttlValue>5</ttlValue>\n" +
                "  <matchPattern>*</matchPattern>\n" +
                "  <fields>field_y,field_z</fields>\n" +
                "  <field_y.ttl>1</field_y.ttl>\n" +
                "  <field_z.ttl>2</field_z.ttl>\n" +
                "</rule>\n";
        // @formatter:on

        AgeOffRuleConfiguration.Builder builder = new AgeOffRuleConfiguration.Builder();
        builder.withIndentation("  ");
        builder.withFilterClass(datawave.iterators.filter.ageoff.FieldAgeOffFilter.class);

        builder.addSimpleElement("ttlUnits", "s");

        builder.addSimpleElement("ttlValue", "5");

        builder.addSimpleElement("matchPattern", "*");
        builder.addSimpleElement("fields", "field_y,field_z");
        builder.addSimpleElement("field_y.ttl", "1");
        builder.addSimpleElement("field_z.ttl", "2");

        assertEquals(fieldAgeOffRule, generateRule(builder));
    }

    private String generateRule(AgeOffRuleConfiguration.Builder builder) throws IOException {
        StringWriter out = new StringWriter();
        AgeOffRuleFormatter generator = new AgeOffRuleFormatter(builder.build());
        generator.format(out);
        return out.toString();
    }
}
