package datawave.webservice.result.keyword;

import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import datawave.marking.Markings;
import datawave.webservice.query.result.event.HasMarkings;
import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultTagCloud.class)
public abstract class TagCloudBase<T,E extends TagCloudEntryBase<E>> implements HasMarkings, Message<T> {
    protected transient Markings<?> markings;
    protected transient boolean intermediateResult;

    @Override
    public Markings<?> getMarkings() {
        return markings;
    }

    @Override
    public void setMarkings(Markings<?> markings) {
        this.markings = markings;
    }

    public boolean isIntermediateResult() {
        return this.intermediateResult;
    }

    public void setIntermediateResult(boolean intermediateResult) {
        this.intermediateResult = intermediateResult;
    }

    public abstract void setMetadata(Map<String,String> metadata);

    public abstract Map<String,String> getMetadata();

    public abstract String getLanguage();

    public abstract void setLanguage(String language);

    public abstract List<E> getTags();

    public abstract void setTags(List<E> tags);
}
