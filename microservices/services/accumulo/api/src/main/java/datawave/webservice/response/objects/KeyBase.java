package datawave.webservice.response.objects;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import datawave.marking.Markings;
import datawave.webservice.query.result.event.HasColumnVisibility;
import datawave.webservice.query.result.event.HasMarkings;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultKey.class)
public abstract class KeyBase implements HasMarkings, HasColumnVisibility {

    /**
     * @deprecated no longer populated; the column visibility is available from {@link #getColumnVisibility()}
     */
    // TODO remove-accumulo-utils
    @Deprecated(forRemoval = true)
    protected Markings<?> markings;

    public abstract String getRow();

    public abstract String getColFam();

    public abstract String getColQual();

    public abstract long getTimestamp();

    public abstract void setRow(String row);

    public abstract void setColFam(String colFam);

    public abstract void setColQual(String colQual);

    public abstract void setTimestamp(long timestamp);

}
