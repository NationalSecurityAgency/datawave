package datawave.webservice.result.keyword;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringWriter;
import java.util.List;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.junit.jupiter.api.Test;

import datawave.marking.AccessExpressionMarkings;

class DefaultTagCloudTest {

    @Test
    void marshalsConcreteMarkings() throws Exception {
        DefaultTagCloud tagCloud = new DefaultTagCloud();
        tagCloud.setMarkings(AccessExpressionMarkings.create("PUBLIC"));

        DefaultTagCloudResponse response = new DefaultTagCloudResponse();
        response.setTagClouds(List.of(tagCloud));

        StringWriter xml = new StringWriter();
        Marshaller marshaller = JAXBContext.newInstance(DefaultTagCloudResponse.class).createMarshaller();
        marshaller.marshal(response, xml);

        assertTrue(xml.toString().contains("PUBLIC"));
    }
}
