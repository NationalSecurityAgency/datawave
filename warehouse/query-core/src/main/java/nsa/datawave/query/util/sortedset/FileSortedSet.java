package nsa.datawave.query.util.sortedset;

import nsa.datawave.webservice.query.exception.DatawaveErrorCode;
import nsa.datawave.webservice.query.exception.QueryException;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.lang3.builder.EqualsBuilder;

/**
 * A sorted set that can be persisted into a file and still be read in its persisted state. The set can always be re-loaded and then all operations will work as
 * expected. This will support null contained in the underlying sets iff a comparator is supplied that can handle null values.
 * 
 * 
 * 
 * @param <E>
 */
public class FileSortedSet<E extends Serializable> implements SortedSet<E> {
    protected boolean persisted = false;
    protected SortedSet<E> set = null;
    // A null entry placeholder
    public static final NullObject NULL_OBJECT = new NullObject();
    
    // The file handler that handles the underlying io
    public SortedSetFileHandler handler;
    
    /**
     * A factory that will provide the input stream and output stream to the same underlying file.
     * 
     * 
     * 
     */
    public static interface SortedSetFileHandler {
        public InputStream getInputStream() throws IOException;
        
        public OutputStream getOutputStream() throws IOException;
        
        public long getSize();
    }
    
    /**
     * A class that represents a null object within the set
     * 
     * 
     * 
     */
    public static class NullObject implements Serializable {
        private static final long serialVersionUID = -5528112099317370355L;
        
        public NullObject() {}
    }
    
    /**
     * Create a file sorted set from another one
     * 
     * @param other
     */
    public FileSortedSet(FileSortedSet<E> other) {
        this.handler = other.handler;
        this.set = new TreeSet<>(other.set);
        this.persisted = other.persisted;
    }
    
    /**
     * Create a persisted sorted set
     * 
     * @param uri
     */
    public FileSortedSet(SortedSetFileHandler handler, boolean persisted) {
        this.handler = handler;
        this.set = new TreeSet<>();
        this.persisted = persisted;
    }
    
    /**
     * Create a persistede sorted set
     * 
     * @param comparator
     * @param uri
     */
    public FileSortedSet(Comparator<? super E> comparator, SortedSetFileHandler handler, boolean persisted) {
        this.handler = handler;
        this.set = new TreeSet<>(comparator);
        this.persisted = persisted;
    }
    
    /**
     * Create an unpersisted sorted set (still in memory)
     * 
     * @param set
     * @param uri
     */
    public FileSortedSet(SortedSet<E> set, SortedSetFileHandler handler) {
        this.handler = handler;
        this.set = new TreeSet<>(set);
        this.persisted = false;
    }
    
    /**
     * This will dump the set to the file, making the set "persisted"
     * 
     * @throws IOException
     */
    public void persist() throws IOException {
        if (!persisted) {
            boolean verified = false;
            Exception failure = null;
            for (int i = 0; i < 10 && !verified; i++) {
                try {
                    ObjectOutputStream stream = getOutputStream();
                    try {
                        stream.writeInt(set.size());
                        for (E t : set) {
                            writeObject(stream, t);
                        }
                    } finally {
                        stream.close();
                    }
                    // verify we wrote at least the size....
                    if (handler.getSize() < 4) {
                        throw new IOException("Failed to verify file existence");
                    }
                    // now verify at least the first 100 objects were written correctly
                    ObjectInputStream inStream = getInputStream();
                    try {
                        int size = inStream.readInt();
                        if (size != set.size()) {
                            throw new IOException("Failed to verify file size was written");
                        }
                        size = Math.min(size, 100);
                        int count = 0;
                        for (E t : set) {
                            count++;
                            E input = readObject(inStream);
                            if (!equals(t, input)) {
                                throw new IOException("Failed to verify element " + count + " was written");
                            }
                            if (count == size) {
                                break;
                            }
                        }
                    } finally {
                        inStream.close();
                    }
                    
                    verified = true;
                } catch (Exception e) {
                    // ok, try again
                    failure = e;
                }
            }
            if (!verified) {
                throw new IOException("Failed to write sorted set", failure);
            }
            set.clear();
            persisted = true;
        }
    }
    
    private boolean equals(Object o1, Object o2) {
        // equals builder handles the various array cases
        return new EqualsBuilder().append(o1, o2).isEquals();
    }
    
    /**
     * This will read the file into an in-memory set, making this file "unpersisted"
     * 
     * @throws IOException
     * @throws ClassNotFoundException
     */
    public void load() throws IOException, ClassNotFoundException {
        if (persisted) {
            try {
                ObjectInputStream stream = getInputStream();
                try {
                    int size = stream.readInt();
                    for (int i = 0; i < size; i++) {
                        set.add(readObject(stream));
                    }
                } finally {
                    stream.close();
                }
            } catch (Exception e) {
                throw new IOException("Unable to read file into a complete set", e);
            }
            persisted = false;
        }
    }
    
    /**
     * Get an input stream
     * 
     * @return the input stream
     * @throws FileNotFoundException
     * @throws IOException
     */
    protected ObjectInputStream getUnbufferedInputStream() throws IOException {
        return new ObjectInputStream(handler.getInputStream());
    }
    
    /**
     * Get an input stream
     * 
     * @return the input stream
     * @throws FileNotFoundException
     * @throws IOException
     */
    protected ObjectInputStream getInputStream() throws IOException {
        return new ObjectInputStream(new BufferedInputStream(handler.getInputStream()));
    }
    
    /**
     * Get an output stream
     * 
     * @return the output stream
     * @throws IOException
     */
    protected ObjectOutputStream getOutputStream() throws IOException {
        return new ObjectOutputStream(new BufferedOutputStream(handler.getOutputStream()));
    }
    
    /**
     * Write T to an object output stream
     * 
     * @param stream
     * @param t
     * @throws IOException
     * @throws ClassNotFoundException
     */
    protected void writeObject(ObjectOutputStream stream, E t) throws IOException {
        if (t == null) {
            stream.writeObject(NULL_OBJECT);
        } else {
            stream.writeObject(t);
        }
    }
    
    /**
     * Read T from an object input stream
     * 
     * @param stream
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @SuppressWarnings("unchecked")
    protected E readObject(ObjectInputStream stream) throws IOException, ClassNotFoundException {
        Object o = stream.readObject();
        if (o instanceof NullObject) {
            return null;
        } else {
            return (E) o;
        }
    }
    
    /**
     * Is this set persisted?
     */
    public boolean isPersisted() {
        return persisted;
    }
    
    @Override
    public int size() {
        if (persisted) {
            try {
                ObjectInputStream stream = getUnbufferedInputStream();
                try {
                    return stream.readInt();
                } finally {
                    stream.close();
                }
            } catch (Exception e) {
                throw new IllegalStateException("Unable to get size from file", e);
            }
        } else {
            return set.size();
        }
    }
    
    @Override
    public boolean isEmpty() {
        return size() == 0;
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean contains(Object o) {
        if (persisted) {
            E t = (E) o;
            for (Iterator<E> it = iterator(); it.hasNext();) {
                if (it.hasNext()) {
                    E next = it.next();
                    if (equals(next, t)) {
                        return true;
                    }
                }
            }
            return false;
        } else {
            return set.contains(o);
        }
    }
    
    @Override
    public Iterator<E> iterator() {
        if (persisted) {
            return new FileIterator();
        } else {
            return set.iterator();
        }
    }
    
    @Override
    public Object[] toArray() {
        if (persisted) {
            try {
                ObjectInputStream stream = getInputStream();
                try {
                    int size = stream.readInt();
                    Object[] data = new Object[size];
                    for (int i = 0; i < size; i++) {
                        data[i] = readObject(stream);
                    }
                    return data;
                } finally {
                    stream.close();
                }
            } catch (Exception e) {
                throw new IllegalStateException("Unable to read file into a complete set", e);
            }
        } else {
            return set.toArray();
        }
    }
    
    @SuppressWarnings({"unchecked"})
    @Override
    public <T> T[] toArray(T[] a) {
        if (persisted) {
            try {
                ObjectInputStream stream = getInputStream();
                try {
                    int size = stream.readInt();
                    if (a.length < size) {
                        a = (T[]) (Array.newInstance(a.getClass().getComponentType(), size));
                    }
                    for (int i = 0; i < size; i++) {
                        a[i] = (T) readObject(stream);
                    }
                    return a;
                } finally {
                    stream.close();
                }
            } catch (Exception e) {
                throw new IllegalStateException("Unable to read file into a complete set", e);
            }
        } else {
            return set.toArray(a);
        }
    }
    
    @Override
    public boolean add(E e) {
        if (persisted) {
            throw new IllegalStateException("Cannot add an element to a persisted FileSortedSet.  Please call load() first.");
        } else {
            return set.add(e);
        }
    }
    
    @Override
    public boolean remove(Object o) {
        if (persisted) {
            throw new IllegalStateException("Cannot remove an element to a persisted FileSortedSet.  Please call load() first.");
        } else {
            return set.remove(o);
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public boolean containsAll(Collection<?> c) {
        if (c.isEmpty()) {
            return true;
        }
        if (persisted) {
            try {
                SortedSet<E> all = new TreeSet<>(set.comparator());
                for (Object o : c) {
                    all.add((E) o);
                }
                ObjectInputStream stream = getInputStream();
                try {
                    int size = stream.readInt();
                    for (int i = 0; i < size; i++) {
                        if (all.remove(readObject(stream))) {
                            if (all.isEmpty()) {
                                return true;
                            }
                        }
                    }
                } finally {
                    stream.close();
                }
            } catch (Exception e) {
                throw new IllegalStateException("Unable to read file into a complete set", e);
            }
            return false;
        } else {
            return set.containsAll(c);
        }
    }
    
    @Override
    public boolean addAll(Collection<? extends E> c) {
        if (persisted) {
            throw new IllegalStateException("Unable to add to a persisted FileSortedSet.  Please call load() first.");
        } else {
            return set.addAll(c);
        }
    }
    
    @Override
    public boolean retainAll(Collection<?> c) {
        if (persisted) {
            throw new IllegalStateException("Unable to modify a persisted FileSortedSet.  Please call load() first.");
        } else {
            return set.retainAll(c);
        }
    }
    
    @Override
    public boolean removeAll(Collection<?> c) {
        if (persisted) {
            throw new IllegalStateException("Unable to remove from a persisted FileSortedSet.  Please call load() first.");
        } else {
            return set.removeAll(c);
        }
    }
    
    @Override
    public void clear() {
        if (persisted) {
            try {
                ObjectOutputStream stream = getOutputStream();
                try {
                    stream.writeInt(0);
                } finally {
                    stream.close();
                }
            } catch (Exception e) {
                throw new IllegalStateException("Unable to clear out file", e);
            }
        } else {
            set.clear();
        }
    }
    
    @Override
    public Comparator<? super E> comparator() {
        return set.comparator();
    }
    
    @Override
    public SortedSet<E> subSet(E fromElement, E toElement) {
        if (persisted) {
            throw new IllegalStateException("Unable to subset a persisted FileSortedSet.  Please call load() first.");
        } else {
            return set.subSet(fromElement, toElement);
        }
    }
    
    @Override
    public SortedSet<E> headSet(E toElement) {
        if (persisted) {
            throw new IllegalStateException("Unable to subset a persisted FileSortedSet.  Please call load() first.");
        } else {
            return set.headSet(toElement);
        }
    }
    
    @Override
    public SortedSet<E> tailSet(E fromElement) {
        if (persisted) {
            throw new IllegalStateException("Unable to subset a persisted FileSortedSet.  Please call load() first.");
        } else {
            return set.tailSet(fromElement);
        }
    }
    
    @Override
    public E first() {
        boolean gotFirst = false;
        E first = null;
        if (persisted) {
            try {
                ObjectInputStream stream = getUnbufferedInputStream();
                try {
                    int size = stream.readInt();
                    if (size != 0) {
                        first = readObject(stream);
                        gotFirst = true;
                    }
                } finally {
                    stream.close();
                }
            } catch (Exception e) {
                throw new IllegalStateException("Unable to get first from file", e);
            }
        } else if (!set.isEmpty()) {
            first = set.first();
            gotFirst = true;
        }
        if (!gotFirst) {
            QueryException qe = new QueryException(DatawaveErrorCode.FETCH_FIRST_ELEMENT_ERROR);
            throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
        } else {
            return first;
        }
    }
    
    @Override
    public E last() {
        boolean gotLast = false;
        E last = null;
        if (persisted) {
            try {
                ObjectInputStream stream = getInputStream();
                try {
                    int size = stream.readInt();
                    while (size > 1) {
                        readObject(stream);
                        size--;
                    }
                    if (size != 0) {
                        last = readObject(stream);
                        gotLast = true;
                    }
                } finally {
                    stream.close();
                }
            } catch (Exception e) {
                throw new IllegalStateException("Unable to get last from file", e);
            }
        } else if (!set.isEmpty()) {
            last = set.last();
            gotLast = true;
        }
        if (!gotLast) {
            QueryException qe = new QueryException(DatawaveErrorCode.FETCH_LAST_ELEMENT_ERROR);
            throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
        } else {
            return last;
        }
    }
    
    /********* Some sub classes ***********/
    
    /**
     * This is the iterator for a persisted FileSortedSet
     * 
     * 
     * 
     */
    protected class FileIterator implements Iterator<E> {
        private int size = 0;
        private int index = 0;
        private ObjectInputStream stream = null;
        
        public FileIterator() {
            try {
                this.stream = getInputStream();
                this.size = stream.readInt();
                if (this.size == 0) {
                    cleanup();
                }
            } catch (Exception e) {
                throw new IllegalStateException("Unable to read file", e);
            }
        }
        
        private void cleanup() {
            if (stream != null) {
                try {
                    stream.close();
                } catch (Exception e) {
                    // we tried...
                }
                stream = null;
            }
        }
        
        @Override
        public boolean hasNext() {
            if (index >= size) {
                cleanup();
            }
            return index < size;
        }
        
        @Override
        public E next() {
            if (index >= size) {
                cleanup();
                QueryException qe = new QueryException(DatawaveErrorCode.FETCH_NEXT_ELEMENT_ERROR);
                throw (NoSuchElementException) (new NoSuchElementException().initCause(qe));
            }
            try {
                E o = readObject(stream);
                index++;
                if (index >= size) {
                    cleanup();
                }
                return o;
            } catch (Exception e) {
                throw new IllegalStateException("Unable to get next element from file", e);
            }
        }
        
        @Override
        public void remove() {
            throw new UnsupportedOperationException("Cannot remove elements from a persisted file.  Please call load() first.");
        }
        
        @Override
        protected void finalize() throws Throwable {
            cleanup();
            super.finalize();
        }
        
    }
    
    /********* Some utilities ***********/
    
    private boolean equals(E o1, E o2) {
        if (o1 == null) {
            return o2 == null;
        } else if (o2 == null) {
            return false;
        } else {
            if (set.comparator() == null) {
                return o1.equals(o2);
            } else {
                return set.comparator().compare(o1, o2) == 0;
            }
        }
    }
    
}
