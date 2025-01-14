package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.time.format.DateTimeParseException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.Iterators;
import com.google.common.collect.Sets;

import datawave.marking.MarkingFunctions;
import datawave.query.Constants;
import datawave.query.collections.FunctionalSet;
import datawave.query.composite.CompositeMetadata;
import datawave.query.function.KeyToFieldName;
import datawave.query.jexl.DatawaveJexlContext;
import datawave.query.jexl.JexlASTHelper;
import datawave.query.predicate.EventDataQueryFilter;
import datawave.query.predicate.ValueToAttributes;
import datawave.query.util.TypeMetadata;
import datawave.util.time.DateHelper;

public class Document extends AttributeBag<Document> implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final Logger log = Logger.getLogger(Document.class);

    public static final String DOCKEY_FIELD_NAME = "RECORD_ID";

    private int _count = 0;
    long _bytes = 0;
    private static final long ONE_HUNDRED_M = 1024L * 1000 * 100;
    private static final long ONE_M = 1024L * 1000;
    private static final long FIVE_HUNDRED_K = 1024L * 500;
    TreeMap<String,Attribute<? extends Comparable<?>>> dict;

    /**
     * should sizes of the documents be tracked
     */
    private boolean trackSizes;

    /**
     * Whether or not this document represents an intermediate result. If true, then the document fields should also be empty.
     */
    private boolean intermediateResult;

    private static final long ONE_DAY_MS = 1000L * 60 * 60 * 24;

    public MarkingFunctions getMarkingFunctions() {
        return MarkingFunctions.Factory.createMarkingFunctions();
    }

    public Map<String,String> getMarkings() {
        try {
            MarkingFunctions markingFunctions = MarkingFunctions.Factory.createMarkingFunctions();
            return markingFunctions.translateFromColumnVisibility(getColumnVisibility());
        } catch (MarkingFunctions.Exception e) {}
        return Collections.emptyMap();
    }

    public Document() {
        this(null, true);
    }

    public Document(Key key, boolean toKeep) {
        this(key, toKeep, true);
    }

    public Document(Key key, boolean toKeep, boolean trackSizes) {
        super(key, toKeep);
        dict = new TreeMap<>();
        this.trackSizes = trackSizes;
    }

    public Document(Key key, Set<Key> docKeys, boolean fromIndex, Iterator<Entry<Key,Value>> iter, TypeMetadata typeMetadata,
                    CompositeMetadata compositeMetadata, boolean includeGroupingContext, boolean keepRecordId, EventDataQueryFilter attrFilter) {
        this(key, docKeys, fromIndex, iter, typeMetadata, compositeMetadata, includeGroupingContext, keepRecordId, attrFilter, true);
    }

    public Document(Key key, Set<Key> docKeys, boolean fromIndex, Iterator<Entry<Key,Value>> iter, TypeMetadata typeMetadata,
                    CompositeMetadata compositeMetadata, boolean includeGroupingContext, boolean keepRecordId, EventDataQueryFilter attrFilter,
                    boolean toKeep) {
        this(key, docKeys, fromIndex, iter, typeMetadata, compositeMetadata, includeGroupingContext, keepRecordId, attrFilter, toKeep, true);
    }

    public Document(Key key, Set<Key> docKeys, boolean fromIndex, Iterator<Entry<Key,Value>> iter, TypeMetadata typeMetadata,
                    CompositeMetadata compositeMetadata, boolean includeGroupingContext, boolean keepRecordId, EventDataQueryFilter attrFilter, boolean toKeep,
                    boolean trackSizes) {
        this(key, toKeep, trackSizes);
        this.consumeRawData(key, docKeys, iter, typeMetadata, compositeMetadata, includeGroupingContext, keepRecordId, attrFilter, fromIndex);
    }

    @Override
    public Collection<Attribute<? extends Comparable<?>>> getAttributes() {
        return Collections.unmodifiableCollection(this.dict.values());
    }

    public Map<String,Attribute<? extends Comparable<?>>> getDictionary() {
        return Collections.unmodifiableMap(this.dict);
    }

    private TreeMap<String,Attribute<? extends Comparable<?>>> _getDictionary() {
        return dict;
    }

    public Set<Entry<String,Attribute<? extends Comparable<?>>>> entrySet() {
        return getDictionary().entrySet();
    }

    public Iterator<Entry<String,Attribute<? extends Comparable<?>>>> iterator() {
        return getDictionary().entrySet().iterator();
    }

    /**
     * Given an iterator over {@code Entry<Key, Value>}, and a set of normalizers, this method will merge the attributes scanned over by the supplied iterator
     * into <code>this</code> Document.
     *
     * @param iter
     *            iterator of entry map
     * @param typeMetadata
     *            the type metadata
     * @param docKey
     *            document key
     * @param attrFilter
     *            attribute filter
     * @param compositeMetadata
     *            the composite metadata
     * @param docKeys
     *            the document keys
     * @param fromIndex
     *            boolean flag for fromIndex
     * @param includeGroupingContext
     *            check for including the grouping context
     * @param keepRecordId
     *            check for keepRecordId
     * @return a Document object
     */
    public Document consumeRawData(Key docKey, Set<Key> docKeys, Iterator<Entry<Key,Value>> iter, TypeMetadata typeMetadata,
                    CompositeMetadata compositeMetadata, boolean includeGroupingContext, boolean keepRecordId, EventDataQueryFilter attrFilter,
                    boolean fromIndex) {
        invalidateMetadata();
        // extract the sharded time from the dockey if possible
        try {
            this.shardTimestamp = DateHelper.parseWithGMT(docKey.getRow().toString()).getTime();
        } catch (DateTimeParseException e) {
            log.warn("Unable to parse document key row as a shard id of the form yyyyMMdd...: " + docKey.getRow(), e);
            // leave the shardTimestamp empty
            this.shardTimestamp = Long.MAX_VALUE;
        }

        // Extract the fieldName from the Key
        Iterator<Entry<Key,String>> extractedFieldNames = Iterators.transform(iter, new KeyToFieldName(includeGroupingContext));

        // Transform the remaining entries back into Attributes
        Iterator<Iterable<Entry<String,Attribute<? extends Comparable<?>>>>> attributes = Iterators.transform(extractedFieldNames,
                        new ValueToAttributes(compositeMetadata, typeMetadata, attrFilter, MarkingFunctions.Factory.createMarkingFunctions(), fromIndex));

        // Add all of the String=>Attribute pairs to this Document
        while (attributes.hasNext()) {
            Iterable<Entry<String,Attribute<? extends Comparable<?>>>> entries = attributes.next();
            for (Entry<String,Attribute<? extends Comparable<?>>> entry : entries) {
                this.put(entry, includeGroupingContext);
            }
        }

        // now add the dockeys as attributes
        Attribute<?> docKeyAttributes = toDocKeyAttributes(docKeys, keepRecordId);
        if (docKeyAttributes != null) {
            this.put(DOCKEY_FIELD_NAME, docKeyAttributes);
        }

        // a little debugging here to track large documents
        debugDocumentSize(docKey);

        return this;
    }

    public Attribute<?> toDocKeyAttributes(Set<Key> docKeys, boolean keepRecordId) {
        Attributes attributes = new Attributes(keepRecordId, trackSizes);
        for (Key docKey : docKeys) {
            // if the attribute filter says not to keep it, then don't even create it.
            attributes.add(new DocumentKey(docKey, keepRecordId));
        }
        if (attributes.size() == 0) {
            return null;
        } else if (attributes.size() == 1) {
            return attributes.getAttributes().iterator().next();
        } else {
            return attributes;
        }
    }

    public void debugDocumentSize(Key docKey) {
        if (log.isDebugEnabled()) {
            log.debug("Document " + docKey + "; size = " + size() + "; bytes = " + sizeInBytes());
        }
    }

    /**
     * Returns true if this <code>Document</code> contains the given <code>key</code>
     *
     * @param key
     *            a key
     * @return a boolean on if a key is found
     */
    public boolean containsKey(String key) {
        return this.dict.containsKey(key);
    }

    /**
     * Fetch the value for the given <code>key</code>. Will return <code>null</code> if no such mapping exists.
     *
     * @param key
     *            the key
     * @return the attribute value
     */
    public Attribute<?> get(String key) {
        return this.dict.get(key);
    }

    public void put(String key, Attribute<?> value) {
        put(key, value, false);
    }

    /**
     * Replaces an attribute within a document
     *
     * @param key
     *            the key
     * @param value
     *            a value
     * @param includeGroupingContext
     *            flag to include grouping context
     */
    public void replace(String key, Attribute<?> value, Boolean includeGroupingContext) {
        dict.put(key, value);
    }

    /**
     * Adds a named attribute to this document. If an attribute already exists under the specified <code>key</code>, then this method will create an
     * <code>Attributes</code> object (if the attribute associated with <code>key</code> isn't already an <code>Attributes</code>), and add the
     * <code>value</code> and existing attribute to that list.
     *
     * @param key
     *            the key
     * @param value
     *            the attribute value
     * @param includeGroupingContext
     *            flag to include grouping context
     */
    public void put(String key, Attribute<?> value, Boolean includeGroupingContext) {

        if (0 == value.size()) {
            if (log.isTraceEnabled()) {
                log.trace("Ignoring Attribute for " + key + " which was empty");
            }

            return;
        }

        key = JexlASTHelper.deconstructIdentifier(key, includeGroupingContext);

        if (log.isTraceEnabled()) {
            log.trace("Loading: " + key + "=" + value);
        }

        Attribute<?> existingAttr = dict.get(key);
        if (existingAttr == null) {
            dict.put(key, value);

            _count += value.size();
            if (trackSizes) {
                _bytes += value.sizeInBytes();
                _bytes += Attribute.sizeInBytes(key);
            }

            invalidateMetadata();
        } else {
            if (!existingAttr.equals(value)) {
                Attributes attrs = null;

                // When calling put() on a Document which already contains an Attributes
                // for a given with another Attributes, issue the equivalent of a putAll() on the new Attributes
                // to not create additional hiearchy inside this Document
                //
                // e.g. Given: {"CONTENT"=>[BODY:foo, HEAD:foo]}.put("CONTENT", [BODY:bar, HEAD:bar, FOOT:bar])
                // We want to get: {"CONTENT"=>[BODY:foo, HEAD:foo, BODY:bar, HEAD:bar, FOOT:bar]}
                // *not*: {"CONTENT"=>[BODY:foo, HEAD:foo, [BODY:bar, HEAD:bar, FOOT:bar]]}

                if (value instanceof Attributes && existingAttr instanceof Attributes) {
                    // merge the two sets
                    attrs = (Attributes) existingAttr;

                    _count -= attrs.size();
                    if (trackSizes) {
                        _bytes -= attrs.sizeInBytes();
                    }

                    attrs.addAll(((Attributes) value).getAttributes());

                    _count += attrs.size();
                    if (trackSizes) {
                        _bytes += attrs.sizeInBytes();
                    }
                } else if (value instanceof Attributes) {
                    _count -= existingAttr.size();
                    if (trackSizes) {
                        _bytes -= existingAttr.sizeInBytes();
                    }

                    // change the existing attr to an attributes
                    HashSet<Attribute<? extends Comparable<?>>> attrsSet = Sets.newHashSet();
                    attrsSet.add(existingAttr);
                    attrsSet.addAll(((Attributes) value).getAttributes());
                    attrs = new Attributes(attrsSet, this.isToKeep(), trackSizes);
                    dict.put(key, attrs);

                    _count += attrs.size();
                    if (trackSizes) {
                        _bytes += attrs.sizeInBytes();
                    }
                } else if (existingAttr instanceof Attributes) {
                    // add the value to the set
                    attrs = (Attributes) existingAttr;

                    // Account for the case where we add more results to an Attributes, but the Attributes
                    // ends up being deduped by the underlying Set
                    // e.g. Adding BODY:term into an Attributes for BODY:[term, term2] should result in a size of 2
                    _count -= attrs.size();
                    if (trackSizes) {
                        _bytes -= attrs.sizeInBytes();
                    }

                    attrs.add(value);

                    _count += attrs.size();
                    if (trackSizes) {
                        _bytes += attrs.sizeInBytes();
                    }
                } else {
                    // create a set out of the two values
                    HashSet<Attribute<? extends Comparable<?>>> attrsSet = Sets.newHashSet();
                    attrsSet.add(existingAttr);
                    attrsSet.add(value);
                    attrs = new Attributes(attrsSet, this.isToKeep(), trackSizes);
                    dict.put(key, attrs);

                    _count += value.size();
                    if (trackSizes) {
                        _bytes += value.sizeInBytes();
                    }
                }

                invalidateMetadata();
            }
            // else, a Document cannot contain the same Field:Value, thus
            // when we find a duplicate value in the same field, we ignore it.
        }
    }

    public void put(Entry<String,Attribute<? extends Comparable<?>>> entry, Boolean includeGroupingContext) {
        // No grouping context in the document.
        this.put(entry.getKey(), entry.getValue(), includeGroupingContext);
    }

    public void putAll(Iterator<Entry<String,Attribute<? extends Comparable<?>>>> iterator, Boolean includeGroupingContext) {
        if (null == iterator) {
            return;
        }

        while (iterator.hasNext()) {
            put(iterator.next(), includeGroupingContext);
        }
    }

    public void putAll(Document other, Boolean includeGroupingContext) {
        if (null == other || null == other.dict || other.dict.isEmpty()) {
            return;
        }

        putAll(other.dict.entrySet().iterator(), includeGroupingContext);
    }

    /**
     * Remove an Attribute, non-recursively, from the internal dictionary
     *
     * @param key
     *            a key
     * @return the dictionary with the key removed
     */
    public Attribute<?> remove(String key) {
        if (this.getDictionary().containsKey(key)) {
            Attribute<?> attr = this.getDictionary().get(key);

            this._count -= attr.size();
            if (trackSizes) {
                this._bytes -= attr.sizeInBytes();
                this._bytes -= Attribute.sizeInBytes(key);
            }
            invalidateMetadata();

            return this._getDictionary().remove(key);
        }

        return null;
    }

    /**
     * Remove all Attributes from the Document (recursively) whose field is the provided key.
     *
     * @param key
     *            a key
     */
    public void removeAll(String key) {
        _removeAll(this._getDictionary(), key);
    }

    private void _removeAll(Map<String,Attribute<? extends Comparable<?>>> dict, String key) {

        Iterator<Entry<String,Attribute<? extends Comparable<?>>>> iter = dict.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<String,Attribute<? extends Comparable<?>>> entry = iter.next();

            if (entry.getKey().equals(key) || entry.getValue() instanceof Document) {
                // Remove the Attribute's size
                this._count -= entry.getValue().size();
                if (trackSizes) {
                    this._bytes -= entry.getValue().sizeInBytes();
                }
                invalidateMetadata();

                if (entry.getKey().equals(key)) {
                    iter.remove();
                    if (trackSizes) {
                        this._bytes -= Attribute.sizeInBytes(key);
                    }
                } else {
                    // Recursively delete if it's a Document
                    Document subDocument = (Document) entry.getValue();
                    subDocument.invalidateMetadata();

                    // Recursive delete
                    subDocument.removeAll(key);

                    // Re-add what's left from this subDocument after
                    // the recursive deletion
                    this._count += subDocument.size();
                    if (trackSizes) {
                        this._bytes += subDocument.sizeInBytes();
                    }
                }
            }
        }
    }

    @Override
    public int size() {
        return _count;
    }

    @Override
    public long sizeInBytes() {
        if (trackSizes) {
            return super.sizeInBytes(40) + _bytes + (this.dict.size() * 24) + 40;
            // 32 for local members
            // 24 for TreeMap.Entry overhead, and members
            // 56 for TreeMap members and overhead
        } else {
            return 1;
        }
    }

    @Override
    public Object getData() {
        return Collections.unmodifiableMap(this.dict);
    }

    @Override
    public Attribute<?> reduceToKeep() {
        for (Iterator<Entry<String,Attribute<? extends Comparable<?>>>> it = dict.entrySet().iterator(); it.hasNext();) {
            Entry<String,Attribute<? extends Comparable<?>>> entry = it.next();
            Attribute<?> attr = entry.getValue();
            _count -= attr.size();
            if (trackSizes) {
                _bytes -= attr.sizeInBytes();
            }

            if (attr.isToKeep()) {
                Attribute<?> newAttr = attr.reduceToKeep();
                if (newAttr == null) {
                    if (trackSizes) {
                        _bytes -= Attribute.sizeInBytes(entry.getKey());
                    }
                    it.remove();
                } else {
                    _count += newAttr.size();
                    if (trackSizes) {
                        _bytes += newAttr.sizeInBytes();
                    }
                    if (attr != newAttr) {
                        entry.setValue(newAttr);
                    }
                }
            } else {
                if (trackSizes) {
                    _bytes -= Attribute.sizeInBytes(entry.getKey());
                }
                it.remove();
            }
        }
        invalidateMetadata();
        return this;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, _count);
        out.writeBoolean(trackSizes);
        WritableUtils.writeVLong(out, _bytes);

        // Write out the number of Attributes we're going to store
        WritableUtils.writeVInt(out, this.dict.size());

        for (Entry<String,Attribute<? extends Comparable<?>>> entry : this.dict.entrySet()) {
            // Write out the field name
            WritableUtils.writeString(out, entry.getKey());

            // Write out the concrete Attribute class
            WritableUtils.writeString(out, entry.getValue().getClass().getName());

            // Defer to the concrete instance to write() itself
            entry.getValue().write(out);
        }

        WritableUtils.writeVLong(out, shardTimestamp);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this._count = WritableUtils.readVInt(in);
        this.trackSizes = in.readBoolean();
        this._bytes = WritableUtils.readVLong(in);

        int numAttrs = WritableUtils.readVInt(in);

        this.dict = new TreeMap<>();

        for (int i = 0; i < numAttrs; i++) {
            // Get the fieldName
            String fieldName = WritableUtils.readString(in);

            // Get the class name for the concrete Attribute
            String attrClassName = WritableUtils.readString(in);
            Class<?> clz;

            // Get the Class for the name of the class of the concrete Attribute
            try {
                clz = Class.forName(attrClassName);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }

            // Assert that Attribute is a super class of this Class
            if (!Attribute.class.isAssignableFrom(clz)) {
                throw new ClassCastException("Found class that was not an instance of Attribute");
            }

            // Get an instance of the concrete Attribute
            Attribute<?> attr;
            try {
                attr = (Attribute<?>) clz.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new IOException(e);
            }

            // Reload the attribute
            attr.readFields(in);

            // Add the attribute back to the Map
            this.dict.put(fieldName, attr);
        }

        this.shardTimestamp = WritableUtils.readVLong(in);

        invalidateMetadata();
    }

    @SuppressWarnings("unchecked")
    @Override
    public int compareTo(Document o) {
        if (size() < o.size()) {
            return -1;
        } else if (size() > o.size()) {
            return 1;
        } else {
            TreeMap<String,Attribute<? extends Comparable<?>>> map1 = _getDictionary();
            TreeMap<String,Attribute<? extends Comparable<?>>> map2 = o._getDictionary();
            Iterator<Entry<String,Attribute<? extends Comparable<?>>>> iter1 = map1.entrySet().iterator();
            Iterator<Entry<String,Attribute<? extends Comparable<?>>>> iter2 = map2.entrySet().iterator();

            // iterate over the Attribute name / Attribute entries
            while (iter1.hasNext() && iter2.hasNext()) {
                Entry<String,Attribute<? extends Comparable<?>>> entry1 = iter1.next();
                Entry<String,Attribute<? extends Comparable<?>>> entry2 = iter2.next();

                // compare the Attribute names
                int keyCmp = entry1.getKey().compareTo(entry2.getKey());
                if (0 == keyCmp) {
                    // the Attribute names are equal, so we must compare the values
                    Attribute<?> v1 = entry1.getValue();
                    Attribute<?> v2 = entry2.getValue();

                    // we can not assume that just because the Attribute names are equal that they are the same subclass of Attribute (and therefore Comparable
                    // with each other)
                    // if the v1 and v2 are different subclasses of Attribute, then we will compare the cannonical names of the respective classes
                    int valCmp;
                    String c1 = v1.getClass().getCanonicalName();
                    String c2 = v2.getClass().getCanonicalName();

                    if (c1.equals(c2)) {
                        // same cannonical class name, so we should be able to call compareTo
                        valCmp = ((Comparable<Attribute<?>>) v1).compareTo(v2);
                    } else {
                        // the cannonical class names are not equal, so the Documents are not equal at this point
                        // we need to return either a -1 or a 1, so compare the cannonical class names
                        valCmp = c1.compareTo(c2);
                    }

                    // if the entries are non-equal return the value, otherwise continue to the next one
                    if (0 != valCmp) {
                        return valCmp;
                    }
                } else {
                    return keyCmp;
                }
            }

            if (!iter1.hasNext() && iter2.hasNext()) {
                return -1;
            } else if (iter1.hasNext() && !iter2.hasNext()) {
                return 1;
            } else {
                return 0;
            }
        }
    }

    @Override
    public boolean equals(Object o) {
        if (null == o) {
            return false;
        }

        if (o instanceof Document) {
            Document other = (Document) o;

            return _getDictionary().equals(other._getDictionary());
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(173, 167);

        for (Entry<String,Attribute<? extends Comparable<?>>> entry : this.dict.entrySet()) {
            hcb.append(entry.hashCode());
        }

        return hcb.toHashCode();
    }

    @SuppressWarnings("unchecked")
    @Override
    public Collection<ValueTuple> visit(Collection<String> queryFieldNames, DatawaveJexlContext context) {
        if (log.isTraceEnabled()) {
            log.trace("queryFieldNames: " + queryFieldNames);
        }
        Set<ValueTuple> children = new FunctionalSet<>();
        Set<ValueTuple> anySet = null;
        if (queryFieldNames.contains(Constants.ANY_FIELD)) {
            anySet = new HashSet<>();
        }
        for (Entry<String,Attribute<? extends Comparable<?>>> entry : this.dict.entrySet()) {
            // For evaluation purposes, all field names have the grouping context
            // ripped off, regardless of whether or not it's beign return to the client.
            // Until grouping-context aware query evaluation is implemented, we always
            // want to remove the grouping-context
            String identifier = JexlASTHelper.rebuildIdentifier(entry.getKey(), false);
            if (!queryFieldNames.isEmpty() && !queryFieldNames.contains(Constants.ANY_FIELD)) {
                if (!queryFieldNames.contains(identifier)) {
                    if (log.isTraceEnabled()) {
                        log.trace("leaving " + identifier + " out of the jexlContext");
                    }
                    continue;
                } else {
                    if (log.isTraceEnabled()) {
                        log.trace("putting " + identifier + " into the jexlContext");
                    }
                }
            } else {
                if (log.isTraceEnabled())
                    log.trace("adding " + identifier + " to the jexlContext");
            }
            Collection<ValueTuple> visitObject = entry.getValue().visit(Collections.singleton(entry.getKey()), context);
            children.addAll(visitObject);
            if (context.has(identifier)) {
                Object o = context.get(identifier);

                // If we have a Set already in the Map
                if (Set.class.isAssignableFrom(o.getClass())) {
                    Set<ValueTuple> set = (Set<ValueTuple>) o;
                    set.addAll(visitObject);
                } else if (ValueTuple.class.isAssignableFrom(o.getClass())) {
                    Set<ValueTuple> newSet = new FunctionalSet<>();
                    newSet.add((ValueTuple) o);
                    newSet.addAll(visitObject);
                    // Add the final set
                    context.set(identifier, newSet);
                } else {
                    throw new IllegalStateException("Found an unexpected value class in the context: " + o.getClass());
                }
            } else {
                // Nothing already in the context, just add the result from the visit() as a functional set
                if (!visitObject.isEmpty()) {
                    Set<ValueTuple> newSet = new FunctionalSet<>();
                    newSet.addAll(visitObject);
                    // Add the final set
                    context.set(identifier, newSet);
                }
            }
            if (anySet != null) {
                anySet.addAll(visitObject);
            }
        }
        // now if we have anything in the anySet, add it to the context
        if (anySet != null && !anySet.isEmpty()) {
            context.set(Constants.ANY_FIELD, anySet);
        }

        // this probably will not be used by anybody as the side-effect of loading the JEXL context is the real result
        return children;
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(this._count, true);
        output.writeBoolean(trackSizes);
        output.writeLong(this._bytes, true);

        output.writeInt(this.dict.size(), true);

        for (Entry<String,Attribute<? extends Comparable<?>>> entry : this.dict.entrySet()) {
            // Write out the field name
            // writeAscii fails to be read correctly if the value has only one character
            // need to use writeString here
            output.writeString(entry.getKey());

            Attribute<?> attribute = entry.getValue();
            output.writeString(attribute.getClass().getName());
            attribute.write(kryo, output);
        }

        output.writeLong(this.shardTimestamp);
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this._count = input.readInt(true);
        trackSizes = input.readBoolean();
        this._bytes = input.readLong(true);

        int numAttrs = input.readInt(true);

        this.dict = new TreeMap<>();

        for (int i = 0; i < numAttrs; i++) {
            // Get the fieldName
            String fieldName = input.readString();

            // Get the class name for the concrete Attribute
            String attrClassName = input.readString();
            Class<?> clz;

            // Get the Class for the name of the class of the concrete Attribute
            try {
                clz = Class.forName(attrClassName);
            } catch (ClassNotFoundException e) {
                throw new RuntimeException(e);
            }

            Attribute<?> attr;
            if (Attribute.class.isAssignableFrom(clz)) {
                // Get an instance of the concrete Attribute
                try {
                    attr = (Attribute<?>) clz.newInstance();
                } catch (InstantiationException | IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

            } else {
                throw new ClassCastException("Found class that was not an instance of Attribute");
            }
            // Reload the attribute
            attr.read(kryo, input);

            // Add the attribute back to the Map
            this.dict.put(fieldName, attr);
        }

        this.shardTimestamp = input.readLong();

        this.invalidateMetadata();
    }

    @Override
    public Document copy() {
        Document d = new Document(this.getMetadata(), this.isToKeep(), trackSizes);

        // _count will be set via put operations
        Set<Entry<String,Attribute<? extends Comparable<?>>>> entries = this._getDictionary().entrySet();
        for (Entry<String,Attribute<? extends Comparable<?>>> entry : entries) {
            d.put(entry.getKey(), (Attribute<?>) entry.getValue().copy());
        }

        d.shardTimestamp = this.shardTimestamp;

        return d;
    }

    public void setIntermediateResult(boolean intermediateResult) {
        this.intermediateResult = intermediateResult;
    }

    public boolean isIntermediateResult() {
        return intermediateResult;
    }

}
