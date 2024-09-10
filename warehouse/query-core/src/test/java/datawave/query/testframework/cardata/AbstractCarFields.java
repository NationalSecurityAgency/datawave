package datawave.query.testframework.cardata;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import datawave.query.testframework.FieldConfig;

public abstract class AbstractCarFields implements FieldConfig {

    private static final Gson gson = new GsonBuilder().setPrettyPrinting().create();

    private final Set<String> indexFields = new HashSet<>();
    private final Set<String> indexOnlyFields = new HashSet<>();
    private final Set<String> reverseIndexFields = new HashSet<>();
    private final Set<String> multivalueFields = new HashSet<>();
    private final Collection<Set<String>> compositeFields = new HashSet<>();
    private final Collection<Set<String>> virtualFields = new HashSet<>();
    private final Set<String> tokenizedFields = new HashSet<>();

    protected AbstractCarFields(Collection<String> index, Collection<String> indexOnly, Collection<String> reverse, Collection<String> multivalue,
                    Collection<Set<String>> composite, Collection<Set<String>> virtual) {
        this.indexFields.addAll(index);
        this.indexOnlyFields.addAll(indexOnly);
        this.reverseIndexFields.addAll(reverse);
        this.multivalueFields.addAll(multivalue);
        this.compositeFields.addAll(composite);
        this.virtualFields.addAll(virtual);
    }

    @Override
    public Set<String> getIndexFields() {
        return this.indexFields;
    }

    @Override
    public void addIndexField(String field) {
        this.indexFields.add(field);
    }

    @Override
    public void removeIndexField(String field) {
        indexFields.remove(field);
    }

    @Override
    public Set<String> getIndexOnlyFields() {
        return this.indexOnlyFields;
    }

    @Override
    public void addIndexOnlyField(String field) {
        this.indexOnlyFields.add(field);
    }

    @Override
    public void removeIndexOnlyField(String field) {
        this.indexOnlyFields.remove(field);
    }

    @Override
    public Set<String> getReverseIndexFields() {
        return this.reverseIndexFields;
    }

    @Override
    public void addReverseIndexField(String field) {
        this.reverseIndexFields.add(field);
    }

    @Override
    public void removeReverseIndexField(String field) {
        this.reverseIndexFields.remove(field);
    }

    @Override
    public Collection<Set<String>> getCompositeFields() {
        return this.compositeFields;
    }

    @Override
    public void addCompositeField(Set<String> field) {
        this.compositeFields.add(field);
    }

    @Override
    public void removeCompositeField(Set<String> field) {
        this.compositeFields.remove(field);
    }

    @Override
    public Collection<Set<String>> getVirtualFields() {
        return this.virtualFields;
    }

    @Override
    public void addVirtualField(Set<String> field) {
        this.virtualFields.add(field);
    }

    @Override
    public void removeVirtualField(Set<String> field) {
        this.virtualFields.remove(field);
    }

    @Override
    public Set<String> getMultiValueFields() {
        return this.multivalueFields;
    }

    @Override
    public void addMultiValueField(String field) {
        this.multivalueFields.add(field);
    }

    @Override
    public void removeMultiValueField(String field) {
        this.multivalueFields.add(field);
    }

    @Override
    public Set<String> getTokenizedFields() {
        return this.tokenizedFields;
    }

    @Override
    public void addTokenizedField(String field) {
        this.tokenizedFields.add(field);
    }

    @Override
    public void removeTokenizedField(String field) {
        this.tokenizedFields.remove(field);
    }

    @Override
    public String toString() {
        return "AbstractCarFields" + gson.toJson(this);
    }
}
