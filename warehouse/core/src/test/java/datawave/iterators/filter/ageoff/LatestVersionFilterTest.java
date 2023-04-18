package datawave.iterators.filter.ageoff;

import datawave.iterators.filter.AgeOffConfigParams;
import datawave.iterators.test.StubbedIteratorEnvironment;
import org.apache.accumulo.core.conf.AccumuloConfiguration;
import org.apache.accumulo.core.conf.DefaultConfiguration;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.DevNull;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.IteratorUtil;
import datawave.iterators.filter.ageoff.LatestVersionFilter.Mode;
import datawave.iterators.filter.ageoff.LatestVersionFilter.VersionFilterConfiguration;
import org.apache.accumulo.core.iterators.OptionDescriber;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static datawave.iterators.filter.ageoff.LatestVersionFilter.DATATYPE_TIMESTAMP_PROPERTY_NAME_PREFIX;
import static datawave.iterators.filter.ageoff.LatestVersionFilterIteratorHarnessTest.createVersionFilterIteratorOptions;
import static datawave.iterators.filter.ageoff.LatestVersionFilter.DATATYPE_LIST_OPTION_NAME;
import static datawave.iterators.filter.ageoff.LatestVersionFilter.DATATYPE_MODE_OPTION_NAME;
import static datawave.iterators.filter.ageoff.LatestVersionFilter.IS_INDEX_TABLE_OPTION_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class LatestVersionFilterTest {
    private LatestVersionFilter filter;
    
    @Before
    public void createFilter() {
        filter = new LatestVersionFilter();
    }
    
    @Test
    public void equalModeIndexTableScanAccept() {
        filter.isIndexTable = true;
        filter.isScanScope = true;
        assertIndexAccept(true, 0L, 0L, Mode.EQUAL);
        assertIndexAccept(false, 0L, 1L, Mode.EQUAL);
        assertIndexAccept(false, 1L, 0L, Mode.EQUAL);
    }
    
    @Test
    public void equalModeIndexTableNonScanAccept() {
        filter.isIndexTable = true;
        filter.isScanScope = false;
        assertIndexAccept(true, 0L, 0L, Mode.EQUAL);
        assertIndexAccept(false, 0L, 1L, Mode.EQUAL);
        assertIndexAccept(false, 1L, 0L, Mode.EQUAL);
    }
    
    @Test
    public void equalModeNonIndexTableScanAccept() {
        filter.isIndexTable = false;
        filter.isScanScope = true;
        assertNonIndexAccept(true, 0L, 0L, Mode.EQUAL);
        assertNonIndexAccept(false, 0L, 1L, Mode.EQUAL);
        assertNonIndexAccept(false, 1L, 0L, Mode.EQUAL);
    }
    
    @Test
    public void equalModeNonIndexTableNonScanAccept() {
        filter.isIndexTable = false;
        filter.isScanScope = false;
        assertNonIndexAccept(true, 0L, 0L, Mode.EQUAL);
        assertNonIndexAccept(false, 0L, 1L, Mode.EQUAL);
        assertNonIndexAccept(false, 1L, 0L, Mode.EQUAL);
    }
    
    @Test
    public void gteModeIndexTableScanAccept() {
        filter.isIndexTable = true;
        filter.isScanScope = true;
        assertIndexAccept(true, 0L, 0L, Mode.GREATER_THAN_OR_EQUAL);
        assertIndexAccept(false, 0L, 1L, Mode.GREATER_THAN_OR_EQUAL);
        assertIndexAccept(true, 1L, 0L, Mode.GREATER_THAN_OR_EQUAL);
    }
    
    @Test
    public void gteModeIndexTableNonScanAccept() {
        filter.isIndexTable = true;
        filter.isScanScope = false;
        assertIndexAccept(true, 0L, 0L, Mode.GREATER_THAN_OR_EQUAL);
        assertIndexAccept(false, 0L, 1L, Mode.GREATER_THAN_OR_EQUAL);
        assertIndexAccept(true, 1L, 0L, Mode.GREATER_THAN_OR_EQUAL);
    }
    
    @Test
    public void gteModeNonIndexTableScanAccept() {
        filter.isIndexTable = false;
        filter.isScanScope = true;
        assertNonIndexAccept(true, 0L, 0L, Mode.GREATER_THAN_OR_EQUAL);
        assertNonIndexAccept(false, 0L, 1L, Mode.GREATER_THAN_OR_EQUAL);
        assertNonIndexAccept(true, 1L, 0L, Mode.GREATER_THAN_OR_EQUAL);
    }
    
    @Test
    public void gteModeNonIndexTableNonScanAccept() {
        filter.isIndexTable = false;
        filter.isScanScope = false;
        assertNonIndexAccept(true, 0L, 0L, Mode.GREATER_THAN_OR_EQUAL);
        assertNonIndexAccept(false, 0L, 1L, Mode.GREATER_THAN_OR_EQUAL);
        assertNonIndexAccept(true, 1L, 0L, Mode.GREATER_THAN_OR_EQUAL);
    }
    
    @Test
    public void undefinedModeIndexTableScanAccept() {
        filter.isIndexTable = true;
        filter.isScanScope = true;
        assertIndexAccept(false, 0L, 0L, Mode.UNDEFINED);
        assertIndexAccept(false, 0L, 1L, Mode.UNDEFINED);
        assertIndexAccept(false, 1L, 0L, Mode.UNDEFINED);
    }
    
    @Test
    public void undefinedModeIndexTableNonScanAccept() {
        filter.isIndexTable = true;
        filter.isScanScope = false;
        assertIndexAccept(true, 0L, 0L, Mode.UNDEFINED);
        assertIndexAccept(true, 0L, 1L, Mode.UNDEFINED);
        assertIndexAccept(true, 1L, 0L, Mode.UNDEFINED);
    }
    
    @Test
    public void undefinedModeNonIndexTableScanAccept() {
        filter.isIndexTable = false;
        filter.isScanScope = true;
        assertNonIndexAccept(false, 0L, 0L, Mode.UNDEFINED);
        assertNonIndexAccept(false, 0L, 1L, Mode.UNDEFINED);
        assertNonIndexAccept(false, 1L, 0L, Mode.UNDEFINED);
    }
    
    @Test
    public void undefinedModeNonIndexTableNonScanAccept() {
        filter.isIndexTable = false;
        filter.isScanScope = false;
        assertIndexAccept(true, 0L, 0L, Mode.UNDEFINED);
        assertIndexAccept(true, 0L, 1L, Mode.UNDEFINED);
        assertIndexAccept(true, 1L, 0L, Mode.UNDEFINED);
    }
    
    @Test
    public void acceptNoTimestampsAtScanForUndefinedMode() {
        VersionFilterConfiguration vfc = createFilterConfiguration(0L, Mode.UNDEFINED);
        filter.isScanScope = true;
        
        assertFalse(filter.accept(0L, vfc));
        assertFalse(filter.accept(1L, vfc));
        assertFalse(filter.accept(-1L, vfc));
    }
    
    @Test
    public void acceptAllTimestampsAtMajcForUndefinedMode() {
        VersionFilterConfiguration vfc = createFilterConfiguration(0L, Mode.UNDEFINED);
        filter.isScanScope = false;
        
        assertTrue(filter.accept(0L, vfc));
        assertTrue(filter.accept(1L, vfc));
        assertTrue(filter.accept(-1L, vfc));
    }
    
    @Test
    public void acceptsCorrectlyAtScanForEqualMode() {
        VersionFilterConfiguration vfc = createFilterConfiguration(0L, Mode.EQUAL);
        filter.isScanScope = true;
        
        assertTrue(filter.accept(0L, vfc));
        assertFalse(filter.accept(1L, vfc));
        assertFalse(filter.accept(-1L, vfc));
    }
    
    @Test
    public void acceptsCorrectlyAtMajcForEqualMode() {
        VersionFilterConfiguration vfc = createFilterConfiguration(0L, Mode.EQUAL);
        filter.isScanScope = false;
        
        assertTrue(filter.accept(0L, vfc));
        assertFalse(filter.accept(1L, vfc));
        assertFalse(filter.accept(-1L, vfc));
    }
    
    @Test
    public void acceptsCorrectlyAtScanForGreaterThanOrEqualMode() {
        VersionFilterConfiguration vfc = createFilterConfiguration(0L, Mode.GREATER_THAN_OR_EQUAL);
        filter.isScanScope = true;
        
        assertTrue(filter.accept(0L, vfc));
        assertTrue(filter.accept(1L, vfc));
        assertFalse(filter.accept(-1L, vfc));
    }
    
    @Test
    public void acceptsCorrectlyAtMajcForGreaterThanOrEqualMode() {
        VersionFilterConfiguration vfc = createFilterConfiguration(0L, Mode.GREATER_THAN_OR_EQUAL);
        filter.isScanScope = false;
        
        assertTrue(filter.accept(0L, vfc));
        assertTrue(filter.accept(1L, vfc));
        assertFalse(filter.accept(-1L, vfc));
    }
    
    @Test
    public void modeParsesFromOptionValue() {
        assertEquals(Mode.EQUAL, Mode.parseOptionValue("eq"));
        assertEquals(Mode.GREATER_THAN_OR_EQUAL, Mode.parseOptionValue("gte"));
        assertEquals(Mode.UNDEFINED, Mode.parseOptionValue("xyz"));
        assertEquals(Mode.UNDEFINED, Mode.parseOptionValue(""));
        assertEquals(Mode.UNDEFINED, Mode.parseOptionValue(null));
    }
    
    @Test
    public void testInitWithNoOptionsNorEnvironment() throws IOException {
        filter.init(new DevNull(), Collections.emptyMap(), new StubbedIteratorEnvironment());
        assertFalse(filter.isScanScope);
        assertFalse(filter.isIndexTable);
        assertEquals(0, filter.dataTypeConfigurations.size());
    }
    
    @Test
    public void testInitForScanEnv() throws IOException {
        filter.init(new DevNull(), Collections.emptyMap(), stubIteratorScope(IteratorUtil.IteratorScope.scan));
        assertTrue(filter.isScanScope);
    }
    
    @Test
    public void testInitForMajcEnv() throws IOException {
        filter.init(new DevNull(), Collections.emptyMap(), stubIteratorScope(IteratorUtil.IteratorScope.majc));
        assertFalse(filter.isScanScope);
    }
    
    @Test
    public void testInitForMincEnv() throws IOException {
        filter.init(new DevNull(), Collections.emptyMap(), stubIteratorScope(IteratorUtil.IteratorScope.minc));
        assertFalse(filter.isScanScope);
    }
    
    @Test
    public void testInitForIndexTableOption() throws IOException {
        filter.init(new DevNull(), optionsMapWithIsIndexTable("true"), new StubbedIteratorEnvironment());
        assertTrue(filter.isIndexTable);
    }
    
    @Test
    public void testInitForIndexTableFalseOption() throws IOException {
        filter.init(new DevNull(), optionsMapWithIsIndexTable("false"), new StubbedIteratorEnvironment());
        assertFalse(filter.isIndexTable);
    }
    
    @Test
    public void testInitForIndexTableInvalidOption() throws IOException {
        filter.init(new DevNull(), optionsMapWithIsIndexTable("bogus"), new StubbedIteratorEnvironment());
        assertFalse(filter.isIndexTable);
    }
    
    @Test
    public void testInitForIterEnvIndexTable() throws IOException {
        filter.init(new DevNull(), Collections.emptyMap(), stubIterEnvGetIndexTable("true"));
        assertTrue(filter.isIndexTable);
    }
    
    @Test
    public void testInitForIterEnvIndexTableFalse() throws IOException {
        filter.init(new DevNull(), Collections.emptyMap(), stubIterEnvGetIndexTable("false"));
        assertFalse(filter.isIndexTable);
    }
    
    @Test
    public void testInitForIterEnvIndexTableInvalid() throws IOException {
        filter.init(new DevNull(), Collections.emptyMap(), stubIterEnvGetIndexTable("bogus"));
        assertFalse(filter.isIndexTable);
    }
    
    @Test
    public void testIndexTableContradictionTrueFalse() throws IOException {
        filter.init(new DevNull(), optionsMapWithIsIndexTable("true"), stubIterEnvGetIndexTable("false"));
        assertTrue("Option has higher precedence", filter.isIndexTable);
    }
    
    @Test
    public void testIndexTableContradictionTrueNull() throws IOException {
        filter.init(new DevNull(), optionsMapWithIsIndexTable("true"), stubIterEnvGetIndexTable(null));
        assertTrue("Option has higher precedence", filter.isIndexTable);
    }
    
    @Test
    public void testIndexTableContradictionFalseTrue() throws IOException {
        filter.init(new DevNull(), optionsMapWithIsIndexTable("false"), stubIterEnvGetIndexTable("true"));
        assertFalse("Option has higher precedence", filter.isIndexTable);
    }
    
    @Test
    public void testIndexTableContradictionFalseNull() throws IOException {
        filter.init(new DevNull(), optionsMapWithIsIndexTable("false"), stubIterEnvGetIndexTable(null));
        assertFalse("Option has higher precedence", filter.isIndexTable);
    }
    
    @Test
    public void testIndexTableContradictionNullTrue() throws IOException {
        filter.init(new DevNull(), optionsMapWithIsIndexTable(null), stubIterEnvGetIndexTable("true"));
        assertTrue("Option has higher precedence", filter.isIndexTable);
    }
    
    @Test
    public void noDatatypeConfigForMissingOptions() throws IOException {
        assertEquals(0, testInitDataTypeConfigurations(Collections.emptyMap(), new StubbedIteratorEnvironment()).size());
    }
    
    @Test
    public void noDatatypeConfigForNullList() throws IOException {
        assertEquals(0, testInitDataTypeConfigurations(createDataTypeListOptions(null), new StubbedIteratorEnvironment()).size());
    }
    
    @Test
    public void noDatatypeConfigForEmptyList() throws IOException {
        assertEquals(0, testInitDataTypeConfigurations(createDataTypeListOptions(""), new StubbedIteratorEnvironment()).size());
    }
    
    @Test
    public void noDatatypeConfigForDelimOnly() throws IOException {
        assertEquals(0, testInitDataTypeConfigurations(createDataTypeListOptions(","), new StubbedIteratorEnvironment()).size());
    }
    
    @Test
    public void parsesSingleDataTypeConfig() throws IOException {
        assertSingleDefaultConfig(testInitDataTypeConfigurations(createDataTypeListOptions("abc"), new StubbedIteratorEnvironment()));
    }
    
    @Test
    public void ignoresLeadingDelimInDataTypeConfig() throws IOException {
        assertSingleDefaultConfig(testInitDataTypeConfigurations(createDataTypeListOptions(",abc"), new StubbedIteratorEnvironment()));
    }
    
    @Test
    public void ignoresTrailingDelimInDataTypeConfig() throws IOException {
        assertSingleDefaultConfig(testInitDataTypeConfigurations(createDataTypeListOptions("abc,"), new StubbedIteratorEnvironment()));
    }
    
    @Test
    public void trimsAndIgnoresDelimInDataTypeConfig() throws IOException {
        assertSingleDefaultConfig(testInitDataTypeConfigurations(createDataTypeListOptions("     , ,abc,"), new StubbedIteratorEnvironment()));
    }
    
    @Test
    public void parsesListOfTwoDataTypesWithoutConfigurations() throws IOException {
        Map<ByteSequence,VersionFilterConfiguration> filters;
        filters = testInitDataTypeConfigurations(createDataTypeListOptions("xyz,abc"), new StubbedIteratorEnvironment());
        assertEquals(2, filters.size());
        assertDefaultConfiguration(filters.get(new ArrayByteSequence("abc")));
        assertDefaultConfiguration(filters.get(new ArrayByteSequence("xyz")));
    }
    
    @Test
    public void parsesListOfTwoDataTypesWithEnvConfigurations() throws IOException {
        Map<ByteSequence,VersionFilterConfiguration> filters;
        Map<String,String> options = new HashMap<>();
        options.put(DATATYPE_LIST_OPTION_NAME, "xyz,abc");
        options.put("abc." + DATATYPE_MODE_OPTION_NAME, "eq");
        options.put("xyz." + DATATYPE_MODE_OPTION_NAME, "gte");
        
        StubbedIteratorEnvironment iterEnv = stubIterEnvAnyGet(Long.toString(10L));
        filters = testInitDataTypeConfigurations(options, iterEnv);
        
        assertEquals(2, filters.size());
        assertConfiguration(10L, Mode.EQUAL, filters.get(new ArrayByteSequence("abc")));
        assertConfiguration(10L, Mode.GREATER_THAN_OR_EQUAL, filters.get(new ArrayByteSequence("xyz")));
    }
    
    @Test
    public void parsesListOfTwoDataTypesWithOptionConfigurations() throws IOException {
        Map<ByteSequence,VersionFilterConfiguration> filters;
        Map<String,String> options = new HashMap<>();
        options.put(DATATYPE_LIST_OPTION_NAME, "xyz,abc,jkl");
        options.put("abc." + DATATYPE_MODE_OPTION_NAME, "eq");
        options.put("xyz." + DATATYPE_MODE_OPTION_NAME, "gte");
        options.put("jkl." + DATATYPE_MODE_OPTION_NAME, "gte");
        options.put(DATATYPE_TIMESTAMP_PROPERTY_NAME_PREFIX + "abc", "8");
        options.put(DATATYPE_TIMESTAMP_PROPERTY_NAME_PREFIX + "xyz", "9");
        // deliberately missing "jkl" timestamp option
        
        // ignores the Iterator Environment settings except when the datatype is missing from Options
        StubbedIteratorEnvironment iterEnv = stubIterEnvAnyGet(Long.toString(10L));
        filters = testInitDataTypeConfigurations(options, iterEnv);
        
        assertEquals(3, filters.size());
        assertConfiguration(8L, Mode.EQUAL, filters.get(new ArrayByteSequence("abc")));
        assertConfiguration(9L, Mode.GREATER_THAN_OR_EQUAL, filters.get(new ArrayByteSequence("xyz")));
        assertConfiguration(10L, Mode.GREATER_THAN_OR_EQUAL, filters.get(new ArrayByteSequence("jkl")));
    }
    
    @Test
    public void createsFilterConfiguration() {
        Map<String,String> emptyOptions = Collections.emptyMap();
        IteratorEnvironment emptyIterEnv = new StubbedIteratorEnvironment();
        ByteSequence emptyDataType = new ArrayByteSequence("");
        ByteSequence dataType = new ArrayByteSequence("xyz");
        
        assertDefaultConfiguration(filter.createFilterConfiguration(emptyOptions, emptyIterEnv, emptyDataType));
        assertDefaultConfiguration(filter.createFilterConfiguration(null, emptyIterEnv, emptyDataType));
        assertDefaultConfiguration(filter.createFilterConfiguration(emptyOptions, null, emptyDataType));
        assertDefaultConfiguration(filter.createFilterConfiguration(emptyOptions, emptyIterEnv, null));
        
        assertDefaultConfiguration(filter.createFilterConfiguration(emptyOptions, stubIterEnvAnyGet(Long.toString(100L)), emptyDataType));
        assertConfiguration(100L, Mode.UNDEFINED, filter.createFilterConfiguration(emptyOptions, stubIterEnvAnyGet(Long.toString(100L)), dataType));
        assertConfiguration(
                        100L,
                        Mode.EQUAL,
                        filter.createFilterConfiguration(Collections.singletonMap("xyz." + DATATYPE_MODE_OPTION_NAME, "eq"),
                                        stubIterEnvAnyGet(Long.toString(100L)), dataType));
        assertConfiguration(
                        100L,
                        Mode.GREATER_THAN_OR_EQUAL,
                        filter.createFilterConfiguration(Collections.singletonMap("xyz." + DATATYPE_MODE_OPTION_NAME, "gte"),
                                        stubIterEnvAnyGet(Long.toString(100L)), dataType));
        assertConfiguration(
                        100L,
                        Mode.UNDEFINED,
                        filter.createFilterConfiguration(Collections.singletonMap("xyz." + DATATYPE_MODE_OPTION_NAME, "lte"),
                                        stubIterEnvAnyGet(Long.toString(100L)), dataType));
        assertConfiguration(0L, Mode.UNDEFINED,
                        filter.createFilterConfiguration(Collections.singletonMap("xyz." + DATATYPE_MODE_OPTION_NAME, "lte"), emptyIterEnv, dataType));
    }
    
    @Test
    public void parsesTimestamp() {
        IteratorEnvironment iterEnv = new StubbedIteratorEnvironment();
        // verify no exceptions are thrown
        assertEquals(0L, filter.getTimestampVersion(Collections.emptyMap(), iterEnv, null));
        byte[] emptyBytes = {};
        assertEquals(0L, filter.getTimestampVersion(Collections.emptyMap(), iterEnv, new ArrayByteSequence(emptyBytes)));
        assertEquals(0L, filter.getTimestampVersion(Collections.emptyMap(), null, new ArrayByteSequence("xyz")));
        
        assertEquals(0L, filter.getTimestampVersion(Collections.emptyMap(), stubIterEnvAnyGet("not a number"), new ArrayByteSequence("xyz")));
        assertEquals(0L, filter.getTimestampVersion(Collections.emptyMap(), stubIterEnvAnyGet(Long.toString(Long.MAX_VALUE) + "0"),
                        new ArrayByteSequence("xyz")));
        assertEquals(Long.MAX_VALUE,
                        filter.getTimestampVersion(Collections.emptyMap(), stubIterEnvAnyGet(Long.toString(Long.MAX_VALUE)), new ArrayByteSequence("xyz")));
        assertEquals(Long.MIN_VALUE,
                        filter.getTimestampVersion(Collections.emptyMap(), stubIterEnvAnyGet(Long.toString(Long.MIN_VALUE)), new ArrayByteSequence("xyz")));
        assertEquals(0L, filter.getTimestampVersion(Collections.emptyMap(), stubIterEnvAnyGet(Long.toString(0L)), new ArrayByteSequence("xyz")));
    }
    
    @Test
    public void parsesIndexTableStatusAsExpected() {
        // option is present
        assertTrue(filter.extractIndexTableStatus(optionsMapWithIsIndexTable("true"), null));
        assertFalse(filter.extractIndexTableStatus(optionsMapWithIsIndexTable("false"), null));
        assertFalse(filter.extractIndexTableStatus(optionsMapWithIsIndexTable("bogus"), null));
        assertFalse(filter.extractIndexTableStatus(optionsMapWithIsIndexTable(null), null));
        assertTrue("options precede table config",
                        filter.extractIndexTableStatus(optionsMapWithIsIndexTable("true"), createIterEnvWithIsIndexTableValue("false")));
        assertFalse("options precede table config",
                        filter.extractIndexTableStatus(optionsMapWithIsIndexTable("false"), createIterEnvWithIsIndexTableValue("true")));
        
        // option is missing
        assertFalse("should tolerate null value for iterEnv", filter.extractIndexTableStatus(Collections.emptyMap(), null));
        assertFalse("should tolerate null value for iterEnv.config()", filter.extractIndexTableStatus(Collections.emptyMap(), new StubbedIteratorEnvironment()));
        assertFalse("should tolerate null value for table property value",
                        filter.extractIndexTableStatus(Collections.emptyMap(), createIterEnvWithIsIndexTableValue(null)));
        assertFalse("should tolerate invalid value for table property value",
                        filter.extractIndexTableStatus(Collections.emptyMap(), createIterEnvWithIsIndexTableValue("bogus")));
        assertTrue(filter.extractIndexTableStatus(Collections.emptyMap(), createIterEnvWithIsIndexTableValue("true")));
    }
    
    private StubbedIteratorEnvironment createIterEnvWithIsIndexTableValue(String value) {
        return new StubbedIteratorEnvironment() {
            @Override
            public AccumuloConfiguration getConfig() {
                return new DefaultConfiguration() {
                    @Override
                    public String get(String property) {
                        return value;
                    }
                };
            }
        };
    }
    
    private Map<String,String> optionsMapWithIsIndexTable(String value) {
        return Collections.singletonMap(IS_INDEX_TABLE_OPTION_NAME, value);
    }
    
    @Test
    public void describeOptions() {
        // mostly just verify there are no exceptions
        OptionDescriber.IteratorOptions describedOptions = new LatestVersionFilter().describeOptions();
        assertNotNull(describedOptions.getNamedOptions());
        assertNull(describedOptions.getUnnamedOptionDescriptions());
    }
    
    @Test
    public void emptyOptionsAreTolerated() {
        assertTrue(new LatestVersionFilter().validateOptions(new HashMap()));
    }
    
    @Test
    public void optionsValidated() {
        assertTrue(new LatestVersionFilter().validateOptions(createVersionFilterIteratorOptions()));
    }
    
    private StubbedIteratorEnvironment stubIterEnvAnyGet(String value) {
        return new StubbedIteratorEnvironment() {
            @Override
            public AccumuloConfiguration getConfig() {
                return new DefaultConfiguration() {
                    @Override
                    public String get(String property) {
                        return value;
                    }
                };
            }
        };
    }
    
    private StubbedIteratorEnvironment stubIterEnvGetIndexTable(String value) {
        return new StubbedIteratorEnvironment() {
            @Override
            public AccumuloConfiguration getConfig() {
                return new DefaultConfiguration() {
                    @Override
                    public String get(String property) {
                        if (property.equals("table.custom." + AgeOffConfigParams.IS_INDEX_TABLE)) {
                            return value;
                        } else
                            return null;
                    }
                };
            }
        };
    }
    
    private void assertIndexAccept(boolean expectedResult, long keyTimestamp, long configuredTimestamp, Mode mode) {
        String dataType = "xyz";
        VersionFilterConfiguration filterConfiguration = createFilterConfiguration(configuredTimestamp, mode);
        filter.dataTypeConfigurations = Collections.singletonMap(new ArrayByteSequence(dataType), filterConfiguration);
        assertEquals(expectedResult, filter.accept(new Key("row", "cf", "0123456789\00" + dataType, keyTimestamp), new Value()));
    }
    
    private void assertNonIndexAccept(boolean expectedResult, long keyTimestamp, long configuredTimestamp, Mode mode) {
        String dataType = "xyz";
        VersionFilterConfiguration filterConfiguration = createFilterConfiguration(configuredTimestamp, mode);
        filter.dataTypeConfigurations = Collections.singletonMap(new ArrayByteSequence(dataType), filterConfiguration);
        assertEquals(expectedResult, filter.accept(new Key("row", dataType + "\000123456789", "cq", keyTimestamp), new Value()));
    }
    
    private VersionFilterConfiguration createFilterConfiguration(long timestampVersion, Mode mode) {
        VersionFilterConfiguration filterConfiguration = new VersionFilterConfiguration();
        filterConfiguration.timestampVersion = timestampVersion;
        filterConfiguration.mode = mode;
        return filterConfiguration;
    }
    
    private StubbedIteratorEnvironment stubIteratorScope(IteratorUtil.IteratorScope minc) {
        return new StubbedIteratorEnvironment() {
            @Override
            public IteratorUtil.IteratorScope getIteratorScope() {
                return minc;
            }
        };
    }
    
    private void assertSingleDefaultConfig(Map<ByteSequence,VersionFilterConfiguration> filters) {
        assertEquals(1, filters.size());
        assertDefaultConfiguration(filters.get(new ArrayByteSequence("abc")));
    }
    
    private Map<String,String> createDataTypeListOptions(String value) {
        return Collections.singletonMap(DATATYPE_LIST_OPTION_NAME, value);
    }
    
    private void assertConfiguration(long timestamp, Mode mode, VersionFilterConfiguration filterConfiguration) {
        assertEquals(timestamp, filterConfiguration.timestampVersion);
        assertEquals(mode, filterConfiguration.mode);
    }
    
    private void assertDefaultConfiguration(VersionFilterConfiguration filterConfiguration) {
        assertConfiguration(0L, Mode.UNDEFINED, filterConfiguration);
    }
    
    private Map<ByteSequence,LatestVersionFilter.VersionFilterConfiguration> testInitDataTypeConfigurations(Map<String,String> options,
                    IteratorEnvironment iterEnv) throws IOException {
        filter.init(null, options, iterEnv);
        return filter.dataTypeConfigurations;
    }
}
