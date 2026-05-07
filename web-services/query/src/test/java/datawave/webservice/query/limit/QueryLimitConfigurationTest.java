package datawave.webservice.query.limit;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.ctc.wstx.stax.WstxInputFactory;
import com.ctc.wstx.stax.WstxOutputFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.xml.XmlMapper;
import com.fasterxml.jackson.dataformat.xml.ser.ToXmlGenerator;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;

/**
 * Tests to verify that there are no issues serializing/deserializing a {@link QueryLimitConfiguration} using jackson.
 */
class QueryLimitConfigurationTest {

    // @formatter:off
    private static final String CONFIG_AS_JSON =
                    "{\n"
                    + "  \"defaultUserQueryLimit\" : 100,\n"
                    + "  \"defaultSystemQueryLimit\" : 1000,\n"
                    + "  \"internalCacheMaxSize\" : 200,\n"
                    + "  \"userConfigs\" : [ {\n"
                    + "    \"userDn\" : \"CN=User A, C=US\",\n"
                    + "    \"queryLimit\" : 50,\n"
                    + "    \"queryLogicGroupLimits\" : {\n"
                    + "      \"EdgeQueryLogic\" : 15,\n"
                    + "      \"EventQueryLogic\" : 5\n"
                    + "    }\n"
                    + "  }, {\n"
                    + "    \"userDn\" : \"CN=User B, C=US\",\n"
                    + "    \"queryLimit\" : 25,\n"
                    + "    \"queryLogicGroupLimits\" : {\n"
                    + "      \"EventQueryLogic\" : 10\n"
                    + "    }\n" + "  } ],\n"
                    + "  \"systemConfigs\" : [ {\n"
                    + "    \"systemPattern\" : \".*Athena\",\n"
                    + "    \"countsAgainstUserLimit\" : true,\n"
                    + "    \"queryLimit\" : 2000,\n"
                    + "    \"queryLogicGroupLimits\" : {\n"
                    + "      \"EdgeQueryLogic\" : 800,\n"
                    + "      \"EventQueryLogic\" : 500\n"
                    + "    }\n"
                    + "  }, {\n"
                    + "    \"systemPattern\" : \".*Artemis\",\n"
                    + "    \"countsAgainstUserLimit\" : false,\n"
                    + "    \"queryLimit\" : 1500,\n"
                    + "    \"queryLogicGroupLimits\" : {\n"
                    + "      \"EventQueryLogic\" : 600\n"
                    + "    }\n"
                    + "  } ],\n"
                    + "  \"queryLogicGroupConfigs\" : [ {\n"
                    + "    \"groupName\" : \"DefaultEdgeQueryLogicLimit\",\n"
                    + "    \"queryLogicPattern\" : \"Edge.*QueryLogic\",\n"
                    + "    \"queryLimit\" : 50\n"
                    + "  }, {\n"
                    + "    \"groupName\" : \"DefaultEventQueryLogicLimit\",\n"
                    + "    \"queryLogicPattern\" : \"Event.*QueryLogic\",\n"
                    + "    \"queryLimit\" : 25\n"
                    + "  } ]\n"
                    + "}";
    // @formatter:on

    // @formatter:off
    private static final String CONFIG_AS_XML =
                    "<?xml version='1.0' encoding='UTF-8'?>\n"
                    + "<QueryLimitConfiguration>\n"
                    + "  <defaultUserQueryLimit>100</defaultUserQueryLimit>\n"
                    + "  <defaultSystemQueryLimit>1000</defaultSystemQueryLimit>\n"
                    + "  <internalCacheMaxSize>200</internalCacheMaxSize>\n"
                    + "  <userConfigs>\n"
                    + "    <userConfigs>\n"
                    + "      <userDn>CN=User A, C=US</userDn>\n"
                    + "      <queryLimit>50</queryLimit>\n"
                    + "      <queryLogicGroupLimits>\n"
                    + "        <EdgeQueryLogic>15</EdgeQueryLogic>\n"
                    + "        <EventQueryLogic>5</EventQueryLogic>\n"
                    + "      </queryLogicGroupLimits>\n"
                    + "    </userConfigs>\n"
                    + "    <userConfigs>\n"
                    + "      <userDn>CN=User B, C=US</userDn>\n"
                    + "      <queryLimit>25</queryLimit>\n"
                    + "      <queryLogicGroupLimits>\n"
                    + "        <EventQueryLogic>10</EventQueryLogic>\n"
                    + "      </queryLogicGroupLimits>\n"
                    + "    </userConfigs>\n"
                    + "  </userConfigs>\n"
                    + "  <systemConfigs>\n"
                    + "    <systemConfigs>\n"
                    + "      <systemPattern>.*Athena</systemPattern>\n"
                    + "      <countsAgainstUserLimit>true</countsAgainstUserLimit>\n"
                    + "      <queryLimit>2000</queryLimit>\n"
                    + "      <queryLogicGroupLimits>\n"
                    + "        <EdgeQueryLogic>800</EdgeQueryLogic>\n"
                    + "        <EventQueryLogic>500</EventQueryLogic>\n"
                    + "      </queryLogicGroupLimits>\n"
                    + "    </systemConfigs>\n"
                    + "    <systemConfigs>\n"
                    + "      <systemPattern>.*Artemis</systemPattern>\n"
                    + "      <countsAgainstUserLimit>false</countsAgainstUserLimit>\n"
                    + "      <queryLimit>1500</queryLimit>\n"
                    + "      <queryLogicGroupLimits>\n"
                    + "        <EventQueryLogic>600</EventQueryLogic>\n"
                    + "      </queryLogicGroupLimits>\n"
                    + "    </systemConfigs>\n"
                    + "  </systemConfigs>\n"
                    + "  <queryLogicGroupConfigs>\n"
                    + "    <queryLogicGroupConfigs>\n"
                    + "      <groupName>DefaultEdgeQueryLogicLimit</groupName>\n"
                    + "      <queryLogicPattern>Edge.*QueryLogic</queryLogicPattern>\n"
                    + "      <queryLimit>50</queryLimit>\n"
                    + "    </queryLogicGroupConfigs>\n"
                    + "    <queryLogicGroupConfigs>\n"
                    + "      <groupName>DefaultEventQueryLogicLimit</groupName>\n"
                    + "      <queryLogicPattern>Event.*QueryLogic</queryLogicPattern>\n"
                    + "      <queryLimit>25</queryLimit>\n"
                    + "    </queryLogicGroupConfigs>\n"
                    + "  </queryLogicGroupConfigs>\n"
                    + "</QueryLimitConfiguration>\n";
    // @formatter:on

    // @formatter:off
    private static final String CONFIG_AS_YAML =
                    "---\n"
                    + "defaultUserQueryLimit: 100\n"
                    + "defaultSystemQueryLimit: 1000\n"
                    + "internalCacheMaxSize: 200\n"
                    + "userConfigs:\n"
                    + "- userDn: \"CN=User A, C=US\"\n"
                    + "  queryLimit: 50\n"
                    + "  queryLogicGroupLimits:\n"
                    + "    EdgeQueryLogic: 15\n"
                    + "    EventQueryLogic: 5\n"
                    + "- userDn: \"CN=User B, C=US\"\n"
                    + "  queryLimit: 25\n"
                    + "  queryLogicGroupLimits:\n"
                    + "    EventQueryLogic: 10\n"
                    + "systemConfigs:\n"
                    + "- systemPattern: \".*Athena\"\n"
                    + "  countsAgainstUserLimit: true\n"
                    + "  queryLimit: 2000\n"
                    + "  queryLogicGroupLimits:\n"
                    + "    EdgeQueryLogic: 800\n"
                    + "    EventQueryLogic: 500\n"
                    + "- systemPattern: \".*Artemis\"\n"
                    + "  countsAgainstUserLimit: false\n"
                    + "  queryLimit: 1500\n"
                    + "  queryLogicGroupLimits:\n"
                    + "    EventQueryLogic: 600\n"
                    + "queryLogicGroupConfigs:\n"
                    + "- groupName: \"DefaultEdgeQueryLogicLimit\"\n"
                    + "  queryLogicPattern: \"Edge.*QueryLogic\"\n"
                    + "  queryLimit: 50\n"
                    + "- groupName: \"DefaultEventQueryLogicLimit\"\n"
                    + "  queryLogicPattern: \"Event.*QueryLogic\"\n"
                    + "  queryLimit: 25\n";
    // @formatter:on

    private QueryLimitConfiguration config;

    @BeforeEach
    void setUp() {
        UserLimitConfiguration userLimit1 = new UserLimitConfiguration();
        userLimit1.setUserDn("CN=User A, C=US");
        userLimit1.setQueryLimit(50);
        Map<String,Integer> userLimit1Map = new LinkedHashMap<>();
        userLimit1Map.put("EdgeQueryLogic", 15);
        userLimit1Map.put("EventQueryLogic", 5);
        userLimit1.setQueryLogicGroupLimits(userLimit1Map);

        UserLimitConfiguration userLimit2 = new UserLimitConfiguration();
        userLimit2.setUserDn("CN=User B, C=US");
        userLimit2.setQueryLimit(25);
        userLimit2.setQueryLogicGroupLimits(Map.of("EventQueryLogic", 10));

        SystemLimitConfiguration systemLimit1 = new SystemLimitConfiguration();
        systemLimit1.setSystemPattern(".*Athena");
        systemLimit1.setQueryLimit(2000);
        systemLimit1.setCountsAgainstUserLimit(true);
        Map<String,Integer> systemLimit1Map = new LinkedHashMap<>();
        systemLimit1Map.put("EdgeQueryLogic", 800);
        systemLimit1Map.put("EventQueryLogic", 500);
        systemLimit1.setQueryLogicGroupLimits(systemLimit1Map);

        SystemLimitConfiguration systemLimit2 = new SystemLimitConfiguration();
        systemLimit2.setSystemPattern(".*Artemis");
        systemLimit2.setQueryLimit(1500);
        systemLimit2.setCountsAgainstUserLimit(false);
        systemLimit2.setQueryLogicGroupLimits(Map.of("EventQueryLogic", 600));

        QueryLogicGroupLimitConfiguration queryLogicGroupLimit1 = new QueryLogicGroupLimitConfiguration();
        queryLogicGroupLimit1.setGroupName("DefaultEdgeQueryLogicLimit");
        queryLogicGroupLimit1.setQueryLimit(50);
        queryLogicGroupLimit1.setQueryLogicPattern("Edge.*QueryLogic");

        QueryLogicGroupLimitConfiguration queryLogicGroupLimit2 = new QueryLogicGroupLimitConfiguration();
        queryLogicGroupLimit2.setGroupName("DefaultEventQueryLogicLimit");
        queryLogicGroupLimit2.setQueryLimit(25);
        queryLogicGroupLimit2.setQueryLogicPattern("Event.*QueryLogic");

        config = new QueryLimitConfiguration();
        config.setDefaultUserQueryLimit(100);
        config.setDefaultSystemQueryLimit(1000);
        config.setUserConfigs(List.of(userLimit1, userLimit2));
        config.setSystemConfigs(List.of(systemLimit1, systemLimit2));
        config.setQueryLogicGroupConfigs(List.of(queryLogicGroupLimit1, queryLogicGroupLimit2));
    }

    private static final JsonMapper jsonMapper = JsonMapper.builder().enable(SerializationFeature.INDENT_OUTPUT).build();

    // Non-builder creation required to make pretty print work.
    private static final XmlMapper xmlMapper = new XmlMapper(new WstxInputFactory(), new WstxOutputFactory());

    private static final YAMLMapper yamlMapper = new YAMLMapper();

    static {
        yamlMapper.enable(SerializationFeature.INDENT_OUTPUT);
    }

    @BeforeAll
    static void beforeAll() {
        xmlMapper.enable(SerializationFeature.INDENT_OUTPUT);
        xmlMapper.enable(ToXmlGenerator.Feature.WRITE_XML_DECLARATION);
    }

    @Test
    void testJsonSerialization() throws JsonProcessingException {
        assertEquals(CONFIG_AS_JSON, jsonMapper.writeValueAsString(config));
    }

    @Test
    void testJsonDeserialization() throws JsonProcessingException {
        assertEquals(config, jsonMapper.readValue(CONFIG_AS_JSON, QueryLimitConfiguration.class));
    }

    @Test
    void testXmlSerialization() throws JsonProcessingException {
        assertEquals(CONFIG_AS_XML, xmlMapper.writeValueAsString(config));
    }

    @Test
    void testXmlDeserialization() throws JsonProcessingException {
        assertEquals(config, xmlMapper.readValue(CONFIG_AS_XML, QueryLimitConfiguration.class));
    }

    @Test
    void testYamlSerialization() throws JsonProcessingException {
        assertEquals(CONFIG_AS_YAML, yamlMapper.writeValueAsString(config));
    }

    @Test
    void testYamlDeserialization() throws JsonProcessingException {
        assertEquals(config, yamlMapper.readValue(CONFIG_AS_YAML, QueryLimitConfiguration.class));
    }
}
