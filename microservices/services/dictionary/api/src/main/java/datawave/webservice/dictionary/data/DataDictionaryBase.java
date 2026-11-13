package datawave.webservice.dictionary.data;

import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

import datawave.webservice.HtmlProvider;
import datawave.webservice.metadata.MetadataFieldBase;
import datawave.webservice.result.BaseResponse;
import datawave.webservice.result.TotalResultsAware;
import io.protostuff.Message;
import jakarta.xml.bind.annotation.XmlAccessType;
import jakarta.xml.bind.annotation.XmlAccessorType;
import jakarta.xml.bind.annotation.XmlSeeAlso;

@XmlAccessorType(XmlAccessType.NONE)
@XmlSeeAlso(DefaultDataDictionary.class)
public abstract class DataDictionaryBase<T,M extends MetadataFieldBase> extends BaseResponse implements TotalResultsAware, Message<T>, HtmlProvider {

    public abstract List<M> getFields();

    public abstract void setFields(Collection<M> fields);

    public abstract String getDataDictionarySystem();

    public abstract void setDataDictionarySystem(String dataDictionarySystem);

    public abstract void setTotalResults(long totalResults);

    public abstract long getTotalResults();

    public abstract String getTitle();

    public abstract String getHeadContent();

    public abstract String getPageHeader();

    public abstract String getMainContent();

    public abstract void transformFields(final Consumer<M> transformer);
}
