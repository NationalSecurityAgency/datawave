package datawave.webservice.query.result.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.Collections;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.namespace.QName;
import javax.xml.transform.stream.StreamSource;

import org.junit.jupiter.api.Test;

import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;

public class DefaultEventTest {

    private static DefaultEvent createEvent() {
        DefaultEvent event = new DefaultEvent();
        event.setColumnVisibility("PRIVATE|(BAR&FOO)");
        event.setFields(Collections.singletonList(new DefaultField("FIELD", "BAR&FOO", 1L, "value")));
        return event;
    }

    @Test
    public void testColumnVisibilityJaxbRoundTrip() throws Exception {
        DefaultEvent event = createEvent();

        JAXBContext context = JAXBContext.newInstance(DefaultEvent.class);
        StringWriter xml = new StringWriter();
        context.createMarshaller().marshal(new JAXBElement<>(new QName("Event"), DefaultEvent.class, event), xml);
        assertTrue(xml.toString().contains("ColumnVisibility>PRIVATE|(BAR&amp;FOO)</"), xml.toString());

        DefaultEvent restored = context.createUnmarshaller().unmarshal(new StreamSource(new StringReader(xml.toString())), DefaultEvent.class).getValue();
        assertEquals("PRIVATE|(BAR&FOO)", restored.getColumnVisibility());
    }

    @Test
    public void testColumnVisibilityProtostuffRoundTrip() {
        DefaultEvent event = createEvent();

        byte[] bytes = ProtostuffIOUtil.toByteArray(event, event.cachedSchema(), LinkedBuffer.allocate());
        DefaultEvent restored = new DefaultEvent();
        ProtostuffIOUtil.mergeFrom(bytes, restored, restored.cachedSchema());

        assertEquals("PRIVATE|(BAR&FOO)", restored.getColumnVisibility());
        assertEquals(event.getFields(), restored.getFields());
    }

    @Test
    public void testColumnVisibilityParticipatesInEquality() {
        DefaultEvent event = createEvent();
        DefaultEvent other = createEvent();
        assertEquals(event, other);
        assertEquals(event.hashCode(), other.hashCode());

        other.setColumnVisibility("PRIVATE");
        assertNotEquals(event, other);
    }
}
