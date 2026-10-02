package datawave.webservice.result.keyword;

import static org.junit.Assert.assertTrue;

import java.io.StringWriter;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.junit.Test;

import datawave.marking.AccessExpressionMarkings;

public class DefaultTagCloudResponseTest {

    @Test
    public void testMarshalMarkings() throws Exception {
        DefaultTagCloud tagCloud = new DefaultTagCloud();
        tagCloud.setMarkings(AccessExpressionMarkings.builder().build());

        DefaultTagCloudResponse response = new DefaultTagCloudResponse();
        response.setTagClouds(List.of(tagCloud));

        StringWriter writer = new StringWriter();
        Marshaller marshaller = JAXBContext.newInstance(DefaultTagCloudResponse.class).createMarshaller();
        marshaller.marshal(response, writer);

        assertTrue(writer.toString().contains("<markings/>"));
    }
}
