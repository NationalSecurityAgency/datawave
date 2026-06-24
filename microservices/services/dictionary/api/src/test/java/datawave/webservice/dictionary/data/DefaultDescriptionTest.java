package datawave.webservice.dictionary.data;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import javax.xml.bind.annotation.XmlSchema;

import org.junit.jupiter.api.Test;

import datawave.marking.AccessExpressionMarkings;

public class DefaultDescriptionTest {

    @Test
    public void testMarshall() throws JAXBException {
        JAXBContext j = JAXBContext.newInstance(DefaultFields.class);

        DefaultFields dicFieldsList = getDefaultFields();

        Marshaller m = j.createMarshaller();
        m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        m.marshal(dicFieldsList, out);

        String result = out.toString();

        assertTrue(result.contains("<columnVisibility>PRIVATE</columnVisibility>"));
        assertTrue(result.contains("<description>my description</description>"));
    }

    private static DefaultFields getDefaultFields() {
        DefaultDescription desc = new DefaultDescription();
        desc.setMarkings(AccessExpressionMarkings.create("PRIVATE"));
        desc.setDescription("my description");

        Set<DefaultDescription> descs = new HashSet<>();
        descs.add(desc);

        DefaultDictionaryField dicField = new DefaultDictionaryField();
        dicField.setDatatype("myType");
        dicField.setFieldName("myField");
        dicField.setDescriptions(descs);

        List<DefaultDictionaryField> dicFields = new ArrayList<>();
        dicFields.add(dicField);

        DefaultFields dicFieldsList = new DefaultFields();
        dicFieldsList.setFields(dicFields);
        return dicFieldsList;
    }

    @Test
    public void testUnmarshall() throws JAXBException {

        String namespace = "";

        for (Annotation a : DefaultDescriptionTest.class.getPackage().getAnnotations()) {
            if (a instanceof XmlSchema) {
                namespace = ((XmlSchema) a).namespace();
            }
        }

        // @formatter:off
        String xmlString = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n"
                        + "<DefaultFieldsResponse xmlns=\"" + namespace + "\">\n"
                        + "    <HasResults>false</HasResults>\n"
                        + "    <OperationTimeMS>0</OperationTimeMS>\n"
                        + "    <Fields>\n"
                        + "        <Field>\n"
                        + "            <datatype>myType</datatype>\n"
                        + "            <descriptions>\n"
                        + "                <description>my description</description>\n"
                        + "                <markings>\n"
                        + "                    <columnVisibility>PRIVATE</columnVisibility>\n"
                        + "                </markings>\n"
                        + "            </descriptions>\n"
                        + "            <fieldName>myField</fieldName>\n"
                        + "        </Field>\n"
                        + "    </Fields>\n"
                        + "    <TotalResults>0</TotalResults>\n"
                        + "</DefaultFieldsResponse>";
        // @formatter:on

        JAXBContext j = JAXBContext.newInstance(DefaultFields.class);
        Unmarshaller u = j.createUnmarshaller();
        DefaultFields resp = (DefaultFields) u.unmarshal(new ByteArrayInputStream(xmlString.getBytes()));
        int fieldCount = 0;
        int descriptionCount = 0;
        int markingCount = 0;

        for (DefaultDictionaryField f : resp.getFields()) {
            assertEquals("myType", f.getDatatype());
            for (DefaultDescription d : f.getDescriptions()) {
                assertEquals("my description", d.getDescription());
                assertTrue(d.getMarkings() instanceof AccessExpressionMarkings);
                AccessExpressionMarkings aem = (AccessExpressionMarkings) d.getMarkings();
                assertEquals("PRIVATE", aem.getColumnVisibilityString());
                descriptionCount++;
            }
            fieldCount++;
        }

        assertEquals(1, fieldCount);
        assertEquals(1, descriptionCount);
    }
}
