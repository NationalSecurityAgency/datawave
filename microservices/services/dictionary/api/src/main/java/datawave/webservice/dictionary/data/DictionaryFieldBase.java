package datawave.webservice.dictionary.data;

import java.util.Set;

import io.protostuff.Message;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlSeeAlso;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultDictionaryField.class)
public abstract class DictionaryFieldBase<T,D extends DescriptionBase> implements Message<T> {

    public abstract String getFieldName();

    public abstract void setFieldName(String fieldName);

    public abstract String getDatatype();

    public abstract void setDatatype(String datatype);

    public abstract Set<D> getDescriptions();

    public abstract void addDescription(D description);

    public abstract void setDescriptions(Set<D> descriptions);

}
