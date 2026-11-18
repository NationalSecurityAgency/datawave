package datawave.webservice.result.keyword;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;

import org.junit.jupiter.api.Test;

import datawave.marking.AccessExpressionMarkings;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;

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

    @Test
    public void roundTripResponseWithColumnVisibility() throws Exception {
        DefaultTagCloud cloud = new DefaultTagCloud();
        cloud.setColumnVisibility("PRIVATE|(BAR&FOO)");
        cloud.setLanguage("en");
        DefaultTagCloudResponse response = new DefaultTagCloudResponse();
        response.setTagClouds(Collections.singletonList(cloud));

        JAXBContext context = JAXBContext.newInstance(DefaultTagCloudResponse.class);
        StringWriter xml = new StringWriter();
        context.createMarshaller().marshal(response, xml);
        DefaultTagCloudResponse restored = (DefaultTagCloudResponse) context.createUnmarshaller().unmarshal(new StringReader(xml.toString()));
        assertEquals("PRIVATE|(BAR&FOO)", restored.getTagClouds().get(0).getColumnVisibility());
    }

    @Test
    public void protostuffRoundTripWithColumnVisibility() {
        DefaultTagCloud cloud = new DefaultTagCloud();
        cloud.setColumnVisibility("PRIVATE|(BAR&FOO)");
        cloud.setLanguage("en");
        cloud.setTags(Collections.emptyList());

        byte[] bytes = ProtostuffIOUtil.toByteArray(cloud, cloud.cachedSchema(), LinkedBuffer.allocate());
        DefaultTagCloud restored = new DefaultTagCloud();
        restored.setTags(Collections.emptyList());
        ProtostuffIOUtil.mergeFrom(bytes, restored, restored.cachedSchema());
        assertEquals("PRIVATE|(BAR&FOO)", restored.getColumnVisibility());
        assertEquals(cloud, restored);
    }
}
