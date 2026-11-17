package datawave.webservice.result.keyword;

import static org.junit.Assert.assertEquals;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.junit.Test;

import datawave.marking.AccessExpressionMarkings;

public class DefaultTagCloudTest {
    @Test
    public void roundTripResponseWithMarkings() throws Exception {
        DefaultTagCloud cloud = new DefaultTagCloud();
        cloud.setMarkings(AccessExpressionMarkings.create("PRIVATE|(BAR&FOO)"));
        cloud.setLanguage("en");
        DefaultTagCloudResponse response = new DefaultTagCloudResponse();
        response.setTagClouds(Collections.singletonList(cloud));

        JAXBContext context = JAXBContext.newInstance(DefaultTagCloudResponse.class);
        Marshaller marshaller = context.createMarshaller();
        StringWriter xml = new StringWriter();
        marshaller.marshal(response, xml);
        DefaultTagCloudResponse restored = (DefaultTagCloudResponse) context.createUnmarshaller().unmarshal(new StringReader(xml.toString()));
        assertEquals("en", restored.getTagClouds().get(0).getLanguage());
        assertEquals("PRIVATE|(BAR&FOO)", ((AccessExpressionMarkings) restored.getTagClouds().get(0).getMarkings()).getColumnVisibilityString());
    }
}
