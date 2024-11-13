package datawave.query.attributes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.io.WritableUtils;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import datawave.marking.MarkingFunctions;
import datawave.query.collections.FunctionalSet;
import datawave.query.jexl.DatawaveJexlContext;

public class Attributes extends AttributeBag<Attributes> implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger log = Logger.getLogger(Attributes.class);
    private Set<Attribute<? extends Comparable<?>>> attributes;
    private int _count = 0;
    // cache the size in bytes as it can be expensive to compute on the fly if we have many attributes
    private long _bytes = super.sizeInBytes(16) + 16 + 48;
    private static final long ONE_DAY_MS = 1000l * 60 * 60 * 24;

    /**
     * Should sizes of documents be tracked
     */
    private boolean trackSizes;

    public MarkingFunctions getMarkingFunctions() {
        return MarkingFunctions.Factory.createMarkingFunctions();
    }

    protected Attributes() {
        this(true);
    }

    public Attributes(boolean toKeep) {
        this(toKeep, true);
    }

    public Attributes(boolean toKeep, boolean trackSizes) {
        super(toKeep);
        attributes = new LinkedHashSet<>();
        this.trackSizes = trackSizes;
    }

    public Attributes(Collection<Attribute<? extends Comparable<?>>> attributes, boolean toKeep) {
        this(attributes, toKeep, true);
    }

    public Attributes(Collection<Attribute<? extends Comparable<?>>> attributes, boolean toKeep, boolean trackSizes) {
        this(toKeep, trackSizes);

        for (Attribute<? extends Comparable<?>> attr : attributes) {
            this.add(attr);
        }
    }

    public Set<Attribute<? extends Comparable<?>>> getAttributes() {
        return Collections.unmodifiableSet(this.attributes);
    }

    private Set<Attribute<? extends Comparable<?>>> _getAttributes() {
        return this.attributes;
    }

    @Override
    public int size() {
        return _count;
    }

    @Override
    public long sizeInBytes() {
        return _bytes;
    }

    public void add(Attribute<? extends Comparable<?>> attr) {
        if (!this.attributes.contains(attr)) {
            this.attributes.add(attr);
            this._count += attr.size();
            if (trackSizes) {
                this._bytes += attr.sizeInBytes() + 24 + 24;
            }
            invalidateMetadata();
        }
    }

    public void addAll(Collection<Attribute<? extends Comparable<?>>> attrs) {
        for (Attribute<? extends Comparable<?>> attr : attrs) {
            this.add(attr);
        }
    }

    @Override
    public Object getData() {
        return Collections.unmodifiableSet(attributes);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeVInt(out, _count);
        out.writeBoolean(trackSizes);
        // Write out the number of Attributes we're going to store
        WritableUtils.writeVInt(out, this.attributes.size());

        for (Attribute<? extends Comparable<?>> attr : this.attributes) {
            // Write out the concrete Attribute class
            WritableUtils.writeString(out, attr.getClass().getName());

            // Defer to the concrete instance to write() itself
            attr.write(out);
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this._count = WritableUtils.readVInt(in);
        this.trackSizes = in.readBoolean();
        int numAttrs = WritableUtils.readVInt(in);
        this.attributes = new LinkedHashSet<>();
        for (int i = 0; i < numAttrs; i++) {
            String attrClassName = WritableUtils.readString(in);
            Class<?> clz;

            // Get the name of the concrete Attribute
            try {
                clz = Class.forName(attrClassName);
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }

            if (!Attribute.class.isAssignableFrom(clz)) {
                throw new ClassCastException("Found class that was not an instance of Attribute");
            }

            // Get the Class for the name of the class of the concrete Attribute
            Attribute<?> attr;
            try {
                attr = (Attribute<?>) clz.getDeclaredConstructor().newInstance();
            } catch (InstantiationException | IllegalAccessException | NoSuchMethodException | InvocationTargetException e) {
                throw new IOException(e);
            }

            // Reload the attribute
            attr.readFields(in);

            // Add the attribute back to the Set
            this.attributes.add(attr);
        }

        this.invalidateMetadata();
    }

    @Override
    public int compareTo(Attributes o) {
        if (_getAttributes().size() < o._getAttributes().size()) {
            return -1;
        } else if (_getAttributes().size() > o._getAttributes().size()) {
            return 1;
        } else {
            Iterator<Attribute<? extends Comparable<?>>> iter1 = _getAttributes().iterator();
            Iterator<Attribute<? extends Comparable<?>>> iter2 = o._getAttributes().iterator();

            while (iter1.hasNext() && iter2.hasNext()) {
                Attribute<?> attr1 = iter1.next();
                Attribute<?> attr2 = iter2.next();

                String c1 = attr1.getClass().getCanonicalName();
                String c2 = attr2.getClass().getCanonicalName();

                // compare class names instead of classes to avoid unintended class inequality issue across class loaders
                // compare cannonical names to avoid unintended class equality between subclasses with the same name in different packages
                if (c1.equals(c2)) {
                    @SuppressWarnings({"rawtypes", "unchecked"})
                    int attrCmp = ((Comparable) attr1).compareTo(attr2);

                    if (0 != attrCmp) {
                        return attrCmp;
                    }
                } else {
                    return c1.compareTo(c2);
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

        if (o instanceof Attributes) {
            Attributes other = (Attributes) o;
            return this.attributes.equals(other.attributes);
        }

        return false;
    }

    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(131, 127);
        for (Attribute<?> a : getAttributes()) {
            hcb.append(a);
        }
        return hcb.toHashCode();
    }

    @Override
    public Collection<ValueTuple> visit(Collection<String> fieldNames, DatawaveJexlContext context) {
        Set<ValueTuple> children = new FunctionalSet<>();
        for (Attribute<?> attr : getAttributes()) {
            children.addAll(attr.visit(fieldNames, context));
        }

        return children;
    }

    @Override
    public Attribute<?> reduceToKeep() {
        Set<Attribute<? extends Comparable<?>>> replacements = new HashSet<>();
        for (Iterator<Attribute<? extends Comparable<?>>> it = this.attributes.iterator(); it.hasNext();) {
            Attribute<?> attr = it.next();
            this._count -= attr.size();
            if (trackSizes) {
                this._bytes -= attr.sizeInBytes() + 24 + 24;
            }
            if (attr.isToKeep()) {
                Attribute<?> newAttr = attr.reduceToKeep();
                if (newAttr == null) {
                    it.remove();
                } else {
                    this._count += newAttr.size();
                    if (trackSizes) {
                        this._bytes += newAttr.sizeInBytes() + 24 + 24;
                    }
                    if (attr != newAttr) {
                        it.remove();
                        replacements.add(newAttr);
                    }
                }
            } else {
                it.remove();
            }
        }
        this.attributes.addAll(replacements);
        invalidateMetadata();

        if (this.attributes.isEmpty()) {
            return null;
        } else if (this.attributes.size() == 1) {
            return this.attributes.iterator().next();
        } else {
            return this;
        }
    }

    @Override
    public void write(Kryo kryo, Output output) {
        output.writeInt(this._count, true);
        output.writeBoolean(this.trackSizes);
        // Write out the number of Attributes we're going to store
        output.writeInt(this.attributes.size(), true);

        for (Attribute<? extends Comparable<?>> attr : this.attributes) {
            // Write out the concrete Attribute class
            output.writeString(attr.getClass().getName());

            // Defer to the concrete instance to write() itself
            attr.write(kryo, output);
        }
    }

    @Override
    public void read(Kryo kryo, Input input) {
        this._count = input.readInt(true);
        this.trackSizes = input.readBoolean();
        int numAttrs = input.readInt(true);

        this.attributes = new LinkedHashSet<>();
        for (int i = 0; i < numAttrs; i++) {
            String attrClassName = input.readString();
            Class<?> clz;

            // Get the name of the concrete Attribute
            try {
                clz = Class.forName(attrClassName);
            } catch (ClassNotFoundException e) {
                log.error("could not find class for \"" + attrClassName + "\"");
                throw new RuntimeException(e);
            }

            if (!Attribute.class.isAssignableFrom(clz)) {
                throw new ClassCastException("Found class that was not an instance of Attribute");
            }

            // Get the Class for the name of the class of the concrete Attribute
            Attribute<?> attr;
            try {
                attr = (Attribute<?>) clz.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                throw new RuntimeException(e);
            }

            // Reload the attribute
            attr.read(kryo, input);

            // Add the attribute back to the Set
            this.attributes.add(attr);
        }

        invalidateMetadata();
    }

    /*
     * (non-Javadoc)
     *
     * @see Attribute#deepCopy()
     */
    @Override
    public Attributes copy() {
        Attributes attrs = new Attributes(this.isToKeep(), this.trackSizes);

        for (Attribute<?> attr : this._getAttributes()) {
            attrs.add((Attribute<?>) attr.copy());
        }

        attrs.setMetadata(getMetadata());
        attrs.validMetadata = this.validMetadata;

        return attrs;
    }

}
