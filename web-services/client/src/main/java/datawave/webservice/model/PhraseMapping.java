package datawave.webservice.model;

import org.apache.commons.lang.builder.CompareToBuilder;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.commons.lang.builder.ToStringBuilder;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlAttribute;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.Serializable;

@XmlRootElement(name = "PhraseMapping")
@XmlAccessorType(XmlAccessType.FIELD)
public class PhraseMapping extends Mapping implements Serializable, Comparable<Mapping> {
    
    private static final long serialVersionUID = 1L;
    public static final String SUBTITUTION = "${sub}";
    
    @XmlAttribute(name = "phrase", required = true)
    private String phrase = null;
    
    public PhraseMapping() {
        super();
    }
    
    public PhraseMapping(String datatype, String fieldName, String modelFieldName, Direction direction, String columnVisibility) {
        super();
        this.datatype = datatype;
        this.modelFieldName = modelFieldName;
        this.columnVisibility = columnVisibility;
        
    }
    
    public String getPhrase() {
        return phrase;
    }
    
    public void setPhrase(String phrase) {
        this.phrase = phrase;
    }
    
    @Override
    public String getProjection() {
        return getPhrase();
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(columnVisibility).append(datatype).append(phrase).append(modelFieldName).toHashCode();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (obj.getClass() != this.getClass())
            return false;
        PhraseMapping other = (PhraseMapping) obj;
        return new EqualsBuilder().append(columnVisibility, other.columnVisibility).append(datatype, other.datatype).append(phrase, other.phrase)
                        .append(modelFieldName, other.modelFieldName).isEquals();
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("columnVisibility", columnVisibility).append("datatype", datatype).append("phrase", phrase)
                        .append("modelFieldName", modelFieldName).toString();
    }
    
    @Override
    public int compareTo(Mapping obj) {
        
        if (obj == null) {
            throw new IllegalArgumentException("can not compare null");
        }
        if (obj instanceof PhraseMapping) {
            if (obj == this)
                return 0;
            
            return new CompareToBuilder().append(datatype, ((PhraseMapping) obj).datatype).append(modelFieldName, ((PhraseMapping) obj).modelFieldName)
                            .append(phrase, ((PhraseMapping) obj).phrase).append(columnVisibility, ((PhraseMapping) obj).columnVisibility).toComparison();
        } else
            return 1;
    }
    
}
