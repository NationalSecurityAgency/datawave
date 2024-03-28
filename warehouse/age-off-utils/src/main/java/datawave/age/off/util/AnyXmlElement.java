package datawave.age.off.util;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBElement;
import javax.xml.bind.annotation.XmlAnyAttribute;
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.namespace.QName;

/**
 * Provides the ability to create a JAXBElement for custom attributes and element name. Credit to
 * https://stackoverflow.com/questions/47587631/how-to-add-dynamic-attribute-to-dynamic-element-in-jaxb
 */
public class AnyXmlElement {
    @XmlAnyAttribute
    private final Map<QName,String> attributes;
    @XmlAnyElement
    private final List<Object> elements;

    public AnyXmlElement() {
        attributes = new LinkedHashMap<>();
        elements = new ArrayList<>();
    }

    public void addAttribute(QName name, String value) {
        attributes.put(name, value);
    }

    public void addElement(Object element) {
        elements.add(element);
    }

    public static JAXBElement<AnyXmlElement> toJAXBElement(QName qName, AnyXmlElement any) {
        return new JAXBElement<>(qName, AnyXmlElement.class, any);
    }
}
