package nsa.datawave.webservice.request;

import java.net.URL;
import java.util.Collections;

import nsa.datawave.webservice.objects.OptionallyEncodedString;
import nsa.datawave.webservice.request.objects.Mutation;
import nsa.datawave.webservice.request.objects.MutationEntry;
import nsa.datawave.webservice.request.objects.TableUpdate;

import org.junit.Assert;
import org.junit.Test;

public class TestUpdateRequest {
    
    /*
     * 
     * <v2:UpdateResponse xmlns:v2="http://service.accumulo/v2"> <v2:Update> <v2:mutationsAccepted>1</v2:mutationsAccepted>
     * <v2:mutationsDenied>0</v2:mutationsDenied> </v2:Update> </v2:UpdateResponse>
     */
    
    @Test
    public void testGenerate() throws Exception {
        UpdateRequest request = new UpdateRequest();
        TableUpdate tableUpdate = new TableUpdate();
        tableUpdate.setTable("test");
        Mutation m = new Mutation();
        MutationEntry me = new MutationEntry();
        me.setColFam(new OptionallyEncodedString("colFam"));
        me.setColQual(new OptionallyEncodedString("colQual"));
        me.setVisibility("ALL");
        me.setValue(new OptionallyEncodedString("myValue"));
        
        m.setMutationEntries(Collections.singletonList(me));
        m.setRow(new OptionallyEncodedString("row"));
        
        tableUpdate.setMutations(Collections.singletonList(m));
        request.setTableUpdates(Collections.singletonList(tableUpdate));
    }
    
    @Test
    public void testUpdate() throws Exception {
        System.out.println(TestUpdateRequest.class.getClassLoader().getResource("."));
        URL request = TestUpdateRequest.class.getResource("/updateRequest2.xml");
        if (null == request)
            Assert.fail("unable to find request file");
    }
    
}
