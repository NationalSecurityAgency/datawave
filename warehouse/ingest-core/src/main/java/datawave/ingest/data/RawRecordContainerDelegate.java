package datawave.ingest.data;

import java.io.DataOutput;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.apache.accumulo.core.security.ColumnVisibility;

import datawave.data.hash.UID;

/**
 * Delegating raw record container
 */
public class RawRecordContainerDelegate implements RawRecordContainer {
    private final RawRecordContainer delegate;

    public RawRecordContainerDelegate(RawRecordContainer delegate) {
        this.delegate = delegate;
    }

    @Override
    public Map<String,String> getSecurityMarkings() {
        return delegate.getSecurityMarkings();
    }

    @Override
    public void setSecurityMarkings(Map<String,String> securityMarkings) {
        delegate.setSecurityMarkings(securityMarkings);
    }

    @Override
    public void addSecurityMarking(String domain, String marking) {
        delegate.addSecurityMarking(domain, marking);
    }

    @Override
    public boolean hasSecurityMarking(String domain, String marking) {
        return delegate.hasSecurityMarking(domain, marking);
    }

    @Override
    public UID getId() {
        return delegate.getId();
    }

    @Override
    public void setId(UID id) {
        delegate.setId(id);
    }

    @Override
    public void generateId(String uidExtra) {
        delegate.generateId(uidExtra);
    }

    @Override
    public Type getDataType() {
        return delegate.getDataType();
    }

    @Override
    public void setDataType(Type dataType) {
        delegate.setDataType(dataType);
    }

    @Override
    public long getDate() {
        return delegate.getDate();
    }

    @Override
    public void setDate(long date) {
        delegate.setDate(date);
    }

    @Override
    public Collection<String> getErrors() {
        return delegate.getErrors();
    }

    @Override
    public void setErrors(Collection<String> errors) {
        delegate.setErrors(errors);
    }

    @Override
    public void addError(String error) {
        delegate.addError(error);
    }

    @Override
    public void removeError(String error) {
        delegate.removeError(error);
    }

    @Override
    public boolean hasError(String error) {
        return delegate.hasError(error);
    }

    @Override
    public boolean fatalError() {
        return delegate.fatalError();
    }

    @Override
    public boolean ignorableError() {
        return delegate.ignorableError();
    }

    @Override
    public void clearErrors() {
        delegate.clearErrors();
    }

    @Override
    public Collection<String> getAltIds() {
        return delegate.getAltIds();
    }

    @Override
    public void setAltIds(Collection<String> altIds) {
        delegate.setAltIds(altIds);
    }

    @Override
    public void addAltId(String altId) {
        delegate.addAltId(altId);
    }

    @Override
    public boolean hasAltId(String altId) {
        return delegate.hasAltId(altId);
    }

    @Override
    public String getRawFileName() {
        return delegate.getRawFileName();
    }

    @Override
    public void setRawFileName(String rawFileName) {
        delegate.setRawFileName(rawFileName);
    }

    @Override
    public long getRawRecordNumber() {
        return delegate.getRawRecordNumber();
    }

    @Override
    public void setRawRecordNumber(long rawRecordNumber) {
        delegate.setRawRecordNumber(rawRecordNumber);
    }

    @Override
    public long getRawFileTimestamp() {
        return delegate.getRawFileTimestamp();
    }

    @Override
    public void setRawFileTimestamp(long rawRecordTimestamp) {
        delegate.setRawFileTimestamp(rawRecordTimestamp);
    }

    @Override
    public byte[] getRawData() {
        return delegate.getRawData();
    }

    @Override
    public void setRawData(byte[] rawData) {
        delegate.setRawData(rawData);
    }

    @Override
    public Object getAuxData() {
        return delegate.getAuxData();
    }

    @Override
    public void setAuxData(Object auxData) {
        delegate.setAuxData(auxData);
    }

    @Override
    public String getAuxProperty(String prop) {
        return delegate.getAuxProperty(prop);
    }

    @Override
    public void setAuxProperty(String prop, String value) {
        delegate.setAuxProperty(prop, value);
    }

    @Override
    public RawRecordContainer copy() {
        return delegate.copy();
    }

    @Override
    public long getDataOutputSize() {
        return delegate.getDataOutputSize();
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        delegate.write(dataOutput);
    }

    @Override
    public ColumnVisibility getVisibility() {
        return delegate.getVisibility();
    }

    @Override
    public void setVisibility(ColumnVisibility visibility) {
        delegate.setVisibility(visibility);
    }

    @Override
    public Date getTimeForUID() {
        return delegate.getTimeForUID();
    }

    @Override
    public boolean isRequiresMasking() {
        return delegate.isRequiresMasking();
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public int hashCode() {
        return delegate.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        return delegate.equals(o);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
