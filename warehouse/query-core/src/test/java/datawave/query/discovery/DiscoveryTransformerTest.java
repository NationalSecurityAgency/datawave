package datawave.query.discovery;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.namespace.QName;

import org.apache.accumulo.core.client.AccumuloClient;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.hadoop.io.MapWritable;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import datawave.accumulo.inmemory.InMemoryAccumuloClient;
import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.core.query.result.event.DefaultResponseObjectFactory;
import datawave.marking.MarkingFunctions;
import datawave.marking.Markings;
import datawave.microservice.query.Query;
import datawave.microservice.query.QueryImpl;
import datawave.query.QueryParameters;
import datawave.query.QueryTestTableHelper;
import datawave.query.composite.CompositeMetadataHelper;
import datawave.query.model.QueryModel;
import datawave.query.util.AllFieldMetadataHelper;
import datawave.query.util.MetadataHelper;
import datawave.query.util.MetadataHelperFactory;
import datawave.query.util.TypeMetadataHelper;
import datawave.table.constants.TableName;
import datawave.webservice.query.result.event.DefaultEvent;
import datawave.webservice.query.result.event.DefaultField;
import datawave.webservice.query.result.event.EventBase;
import datawave.webservice.query.result.event.FieldBase;
import datawave.webservice.query.result.event.Metadata;

public class DiscoveryTransformerTest {
    private static final String METADATA_TABLE_NAME = "DatawaveMetadata";
    private MetadataHelper helper;
    private AllFieldMetadataHelper allFieldHelper;
    private final String[] authorizations = {"FOO", "BAR"};
    private static AccumuloClient client;
    private DiscoveryLogic logic;

    @BeforeAll
    public static void beforeAll() throws Exception {
        InMemoryInstance instance = new InMemoryInstance(DiscoveryTransformerTest.class.getName());
        client = new InMemoryAccumuloClient("", instance);
        client.tableOperations().create(METADATA_TABLE_NAME);
    }

    @BeforeEach
    public void setup() throws Throwable {
        initLogic();
    }

    private void initLogic() {
        logic = new DiscoveryLogic();
        logic.setIndexTableName(TableName.SHARD_INDEX);
        logic.setReverseIndexTableName(TableName.SHARD_RINDEX);
        logic.setModelTableName(QueryTestTableHelper.METADATA_TABLE_NAME);
        logic.setMetadataTableName(QueryTestTableHelper.METADATA_TABLE_NAME);
        logic.setModelName("DATAWAVE");
        logic.setFullTableScanEnabled(false);
        logic.setMaxResults(-1);
        logic.setMaxWork(-1);
        logic.setAllowLeadingWildcard(true);
        logic.setResponseObjectFactory(new DefaultResponseObjectFactory());
        logic.setMarkingFunctions(new MarkingFunctions.Default());
        logic.setMetadataHelperFactory(new MetadataHelperFactory());
    }

    @BeforeEach
    public void beforeEach() {
        allFieldHelper = createAllFieldMetadataHelper();
        helper = createMetadataHelper(allFieldHelper);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testTermsOnlyDiscoveredThing() throws TableNotFoundException {

        // Create settings.
        Query settings = new QueryImpl();
        settings.setQuery("event:20241218_0/samplecsv/1.2.3");
        settings.addParameter(QueryParameters.DECODE_VIEW, "true");
        settings.setQueryAuthorizations("A");
        Map<String,String> parameters = new HashMap<>();
        parameters.put(DiscoveryLogic.VALUES_ONLY, "true");
        settings.addParameters(parameters);

        // Create query model.
        QueryModel qm = helper.getQueryModel(METADATA_TABLE_NAME, "TEST_MODEL");

        // Create transformer.
        DiscoveryTransformer transformer = new DiscoveryTransformer(logic, settings, qm);

        DiscoveredThing thing = new DiscoveredThing("onyx", "", "", "", "FOO", 0L, new MapWritable());

        EventBase expectedEventBase = new DefaultEvent();
        expectedEventBase.setMetadata(discoveredThingToMetadata(thing));
        Markings<?> markings = markingsFromVisibility.apply(thing.getColumnVisibility());

        expectedEventBase.setMarkings(markings);
        expectedEventBase.setFields(discoveredThingToFieldValuesOnly(thing, markings));
        expectedEventBase.setSizeInBytes(6L);

        // This is the heart of the test.
        EventBase eb = transformer.transform(thing);

        String serializeExpectedEvent = serializeEvent.apply(expectedEventBase);
        String serializedEvent = serializeEvent.apply(eb);
        assertEquals(serializeExpectedEvent, serializedEvent);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testTermsOnlySingleDiscoveredThingLoaded() throws TableNotFoundException {
        // Create settings.
        Query settings = new QueryImpl();
        settings.setQuery("event:20241218_0/samplecsv/1.2.3");
        settings.addParameter(QueryParameters.DECODE_VIEW, "true");
        settings.setQueryAuthorizations("A");
        Map<String,String> parameters = new HashMap<>();
        parameters.put(DiscoveryLogic.VALUES_ONLY, "true");
        settings.addParameters(parameters);

        // Create query model.
        QueryModel qm = helper.getQueryModel(METADATA_TABLE_NAME, "TEST_MODEL");

        // Create transformer.
        DiscoveryTransformer transformer = new DiscoveryTransformer(logic, settings, qm);

        DiscoveredThing thing = new DiscoveredThing("bbc", "NETWORK", "csv", "20130101", "FOO", 240L, new MapWritable());

        EventBase expectedEventBase = new DefaultEvent();
        expectedEventBase.setMetadata(discoveredThingToMetadata(thing));
        Markings<?> markings = markingsFromVisibility.apply(thing.getColumnVisibility());

        expectedEventBase.setMarkings(markings);
        expectedEventBase.setFields(discoveredThingToFieldValuesOnly(thing, markings));
        expectedEventBase.setSizeInBytes(6L);

        // This is the heart of the test.
        EventBase eb = transformer.transform(thing);

        String serializeExpectedEvent = serializeEvent.apply(expectedEventBase);
        String serializedEvent = serializeEvent.apply(eb);
        assertEquals(serializeExpectedEvent, serializedEvent);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void testSingleDiscoveredThing() throws TableNotFoundException {
        // Create settings.
        Query settings = new QueryImpl();
        settings.setQuery("event:20241218_0/samplecsv/1.2.3");
        settings.addParameter(QueryParameters.DECODE_VIEW, "true");
        settings.setQueryAuthorizations("A");
        Map<String,String> parameters = new HashMap<>();
        parameters.put(DiscoveryLogic.VALUES_ONLY, "false");
        settings.addParameters(parameters);

        // Create query model.
        QueryModel qm = helper.getQueryModel(METADATA_TABLE_NAME, "TEST_MODEL");

        // Create transformer.
        DiscoveryTransformer transformer = new DiscoveryTransformer(logic, settings, qm);

        DiscoveredThing thing = new DiscoveredThing("bbc", "NETWORK", "csv", "20130101", "FOO", 240L, new MapWritable());

        EventBase expectedEventBase = new DefaultEvent();
        expectedEventBase.setMetadata(discoveredThingToMetadata(thing));
        Markings<?> markings = markingsFromVisibility.apply(thing.getColumnVisibility());

        expectedEventBase.setMarkings(markings);
        expectedEventBase.setFields(discoveredThingToField(thing, markings));
        expectedEventBase.setSizeInBytes(6L);

        // This is the heart of the test.
        EventBase eb = transformer.transform(thing);

        String serializeExpectedEvent = serializeEvent.apply(expectedEventBase);
        String serializedEvent = serializeEvent.apply(eb);
        assertEquals(serializeExpectedEvent, serializedEvent);
    }

    @SuppressWarnings("rawtypes")
    private final Function<EventBase,String> serializeEvent = (x) -> {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            JAXBContext jaxbContext = JAXBContext.newInstance(EventBase.class);
            Marshaller marshaller = jaxbContext.createMarshaller();

            // Construct a root element in order to unmarshall.
            JAXBElement<EventBase> root = new JAXBElement<>(new QName("", "event"), EventBase.class, x);
            marshaller.marshal(root, bos);
        } catch (JAXBException e) {
            throw new RuntimeException(e);
        }

        return bos.toString();
    };

    private final Function<String,Markings<?>> markingsFromVisibility = x -> {
        try {
            return this.logic.getMarkingFunctions().translateFromColumnVisibility(new ColumnVisibility(x));
        } catch (MarkingFunctions.Exception e) {
            throw new RuntimeException("could not parse to markings: " + x);
        }
    };

    @SuppressWarnings("rawtypes")
    private List<FieldBase> discoveredThingToFieldValuesOnly(DiscoveredThing thing, Markings<?> markings) {
        List<FieldBase> fields = new ArrayList<>();

        if (Optional.ofNullable(thing.getTerm()).isPresent()) {
            fields.add(makeField("VALUE", markings, "", 0L, thing.getTerm()));
        }
        return fields;
    }

    @SuppressWarnings("rawtypes")
    private List<FieldBase> discoveredThingToField(DiscoveredThing thing, Markings<?> markings) {
        List<FieldBase> fields = new ArrayList<>();

        if (Optional.ofNullable(thing.getTerm()).isPresent()) {
            fields.add(makeField("VALUE", markings, "", 0L, thing.getTerm()));
        }

        if (Optional.ofNullable(thing.getField()).isPresent()) {
            fields.add(makeField("FIELD", markings, "", 0L, thing.getField()));
        }

        if (Optional.ofNullable(thing.getDate()).isPresent()) {
            fields.add(makeField("DATE", markings, "", 0L, thing.getDate()));
        }

        if (Optional.ofNullable(thing.getType()).isPresent()) {
            fields.add(makeField("DATA TYPE", markings, "", 0L, thing.getType()));
        }

        if (thing.getCount() > 0L) {
            fields.add(makeField("RECORD COUNT", markings, "", 0L, String.valueOf(thing.getCount())));
        }

        return fields;
    }

    private AllFieldMetadataHelper createAllFieldMetadataHelper() {
        final Set<Authorizations> allAuths = Collections.singleton(new Authorizations(authorizations));
        final Set<Authorizations> auths = Collections.singleton(new Authorizations(authorizations));
        TypeMetadataHelper typeMetadataHelper = new TypeMetadataHelper(new HashMap<>(), auths, client, METADATA_TABLE_NAME, auths, false);
        CompositeMetadataHelper compositeMetadataHelper = new CompositeMetadataHelper(client, METADATA_TABLE_NAME, auths);
        return new AllFieldMetadataHelper(typeMetadataHelper, compositeMetadataHelper, client, METADATA_TABLE_NAME, auths, allAuths);
    }

    @SuppressWarnings("rawtypes")
    private FieldBase makeField(String name, Markings<?> markings, String columnVisibility, Long timestamp, Object value) {
        FieldBase field = new DefaultField();
        field.setName(name);
        field.setMarkings(markings);
        field.setColumnVisibility(columnVisibility);
        field.setTimestamp(timestamp);
        field.setValue(value);
        return field;
    }

    private Metadata discoveredThingToMetadata(DiscoveredThing thing) {
        Metadata metadata = new Metadata();
        metadata.setInternalId("");
        metadata.setDataType(thing.getType());
        metadata.setRow(thing.getTerm());
        metadata.setTable(logic.getTableName());
        return metadata;
    }

    private MetadataHelper createMetadataHelper(AllFieldMetadataHelper allFieldHelper) {
        if (allFieldHelper == null) {
            allFieldHelper = createAllFieldMetadataHelper();
        }

        Set<Authorizations> userAuths = Collections.singleton(new Authorizations(authorizations));
        Set<Authorizations> metadataAuths = Collections.singleton(new Authorizations(authorizations));
        return new MetadataHelper(allFieldHelper, metadataAuths, client, METADATA_TABLE_NAME, userAuths, metadataAuths);
    }
}
