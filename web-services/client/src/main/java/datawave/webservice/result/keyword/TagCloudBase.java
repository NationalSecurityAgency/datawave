package datawave.webservice.result.keyword;

import java.util.List;
import java.util.Map;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlSeeAlso;

import com.google.common.collect.Maps;

import datawave.webservice.query.result.event.HasMarkings;
import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultTagCloud.class)
public abstract class TagCloudBase<T,E extends TagCloudEntryBase<E>> implements HasMarkings, Message<T> {
    protected transient Map<String,String> markings;
    protected transient boolean intermediateResult;

    public Map<String,String> getMarkings() {
        assureMarkings();
        return markings;
    }

    protected void assureMarkings() {
        if (this.markings == null)
            this.markings = Maps.newHashMap();
    }

    public void setMarkings(Map<String,String> markings) {
        this.markings = markings;
    }

    public boolean isIntermediateResult() {
        return this.intermediateResult;
    }

    public void setIntermediateResult(boolean intermediateResult) {
        this.intermediateResult = intermediateResult;
    }

    public abstract String getLanguage();

    public abstract void setLanguage(String language);

    public abstract List<E> getTags();

    public abstract void setTags(List<E> tags);
}
