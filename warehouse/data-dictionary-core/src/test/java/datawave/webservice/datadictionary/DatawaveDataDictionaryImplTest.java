package datawave.webservice.datadictionary;

import datawave.accumulo.inmemory.InMemoryInstance;
import datawave.marking.MarkingFunctions;
import datawave.marking.MarkingFunctionsFactory;
import datawave.webservice.results.datadictionary.DefaultDescription;
import datawave.webservice.results.datadictionary.DefaultDictionaryField;
import datawave.webservice.results.datadictionary.DefaultFields;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.junit.Before;
import org.junit.Test;
import org.powermock.reflect.Whitebox;

import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class DatawaveDataDictionaryImplTest {
    
    private final String model = "model";
    private final String modelTable = "modelTable";
    private final String metaTable = "metaTable";
    
    private final String[] auths = {"PRIVATE"};
    
    Set<Authorizations> setOfAuthObjs = Collections.singleton(new Authorizations(auths));
    
    private final MarkingFunctions markingFunctions = new MarkingFunctions.Default();
    private final MarkingFunctionsFactory mFFactory = new MarkingFunctionsFactory();
    private Connector connector;
    
    private InMemoryInstance instance = new InMemoryInstance();
    
    private DatawaveDataDictionaryImpl impl;
    
    @Before
    public void setup() throws URISyntaxException, AccumuloException, AccumuloSecurityException, TableExistsException {
        connector = instance.getConnector("root", new PasswordToken(""));
        connector.securityOperations().changeUserAuthorizations("root", new Authorizations(auths));
        connector.tableOperations().create(metaTable);
        connector.tableOperations().create(modelTable);
    }
    
    @Test
    public void testSetDescription() throws Exception {
        
        Map<String,String> markings = new HashMap<>();
        markings.put("columnVisibility", "PRIVATE");
        
        DefaultDescription desc = new DefaultDescription();
        desc.setMarkings(markings);
        desc.setDescription("my ultra cool description");
        
        Set<DefaultDescription> descs = new HashSet<>();
        descs.add(desc);
        
        DefaultDictionaryField dicField = new DefaultDictionaryField();
        dicField.setDatatype("myType");
        dicField.setFieldName("myField");
        dicField.setDescriptions(descs);
        
        List<DefaultDictionaryField> dicFields = new ArrayList<>();
        dicFields.add(dicField);
        
        DefaultFields fields = new DefaultFields();
        fields.setFields(dicFields);
        
        impl = new DatawaveDataDictionaryImpl();
        
        Whitebox.getField(DatawaveDataDictionaryImpl.class, "allMetadataAuths").set(impl, this.setOfAuthObjs);
        Whitebox.getField(MarkingFunctionsFactory.class, "markingFunctions").set(mFFactory, this.markingFunctions);
        impl.setDescription(connector, metaTable, setOfAuthObjs, model, modelTable, dicField);
        
        Scanner s = connector.createScanner(metaTable, new Authorizations(auths));
        
        for (Map.Entry<Key,Value> entry : s) {
            assertEquals("PRIVATE", entry.getKey().getColumnVisibility().toString());
        }
    }
}
