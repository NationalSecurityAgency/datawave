package datawave.webservice.query.result.event;

import java.util.List;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;

import io.protostuff.Message;

@XmlAccessorType(XmlAccessType.NONE)
public abstract class EventBase<T,F extends FieldBase<F>> implements HasColumnVisibility, Message<T> {

    protected String columnVisibility;

    protected transient boolean intermediateResult;

    protected transient boolean reachedMaxIntermediateResults;

    public abstract Metadata getMetadata();

    public abstract void setMetadata(Metadata metadata);

    /**
     * @param size
     *            the approximate size of this event in bytes
     */
    public abstract void setSizeInBytes(long size);

    /**
     * @return The set size in bytes, -1 if unset
     */
    public abstract long getSizeInBytes();

    /**
     * Get the approximate size of this event in bytes. Used by the ObjectSizeOf mechanism in the webservice. Throws an exception if the local size was not set
     * to allow the ObjectSizeOf mechanism to do its thang.
     *
     * @return the size in bytes
     */
    public abstract long sizeInBytes();

    public abstract void setFields(List<F> fields);

    public abstract List<F> getFields();

    @Override
    public String getColumnVisibility() {
        return columnVisibility;
    }

    @Override
    public void setColumnVisibility(String columnVisibility) {
        this.columnVisibility = columnVisibility;
    }

    public boolean isIntermediateResult() {
        return this.intermediateResult;
    }

    public void setIntermediateResult(boolean intermediateResult) {
        this.intermediateResult = intermediateResult;
    }

    public boolean hasReachedMaxIntermediateResults() {
        return reachedMaxIntermediateResults;
    }

    public void setReachedMaxIntermediateResults(boolean reachedMaxIntermediateResults) {
        this.reachedMaxIntermediateResults = reachedMaxIntermediateResults;
    }
}
