package datawave.webservice.result.keyword;

import java.util.Collection;

import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlSeeAlso;

import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultTagCloudEntry.class)
public abstract class TagCloudEntryBase<T> implements Message<T> {
    public abstract int getFrequency();

    public abstract void setFrequency(int frequency);

    public abstract double getScore();

    public abstract void setScore(double score);

    public abstract Collection<String> getSources();

    public abstract void setSources(Collection<String> sources);

    public abstract String getTerm();

    public abstract void setTerm(String term);
}
