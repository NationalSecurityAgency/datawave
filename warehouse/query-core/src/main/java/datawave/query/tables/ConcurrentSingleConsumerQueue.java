package datawave.query.tables;

import java.util.AbstractQueue;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * An unbounded thread-safe {@linkplain Queue queue} based on array and linked nodes. This queue
 * differs from {@link java.util.concurrent.ConcurrentLinkedQueue} in that it does not support
 * multiple consumer threads.
 * <p>
 * This queue orders elements FIFO (first-in-first-out). The <em>head</em> of the queue is that
 * element that has been on the queue the longest time. The <em>tail</em> of the queue is that
 * element that has been on the queue the shortest time. New elements are inserted at the tail of
 * the queue, and the queue retrieval operations obtain elements at the head of the queue.
 * <p>
 * A {@code ConcurrentSingleConsumerQueue} is an appropriate choice when many threads produce into
 * a common collection and, at a given instant, only a single thread consumes from the collection.
 * This queue does not permit {@code null} elements.
 *
 * @author ben.manes@gmail.com (Ben Manes)
 * @param <E> the type of elements held in this collection
 */
public class ConcurrentSingleConsumerQueue<E> extends AbstractQueue<E> {
    // TODO(bmanes): This implementation is experimental, as it contains fundamental problems with
    // maintaining FIFO order. In particular the transition in and out of the link node based queue
    // is questionable.

    static int ceilingNextPowerOfTwo(int x) {
        // From Hacker's Delight, Chapter 3, Harry S. Warren Jr.
        return 1 << (Integer.SIZE - Integer.numberOfLeadingZeros(x - 1));
    }

    final AtomicReferenceArray<E> array;
    final AtomicLong head;
    final AtomicLong tail;
    final int mask;

    Node<E> headNode;
    final AtomicReference<Node<E>> tailNode;

    public ConcurrentSingleConsumerQueue(int estimatedCapacity) {
        if (estimatedCapacity <= 0) {
            throw new IllegalArgumentException();
        }
        array = new AtomicReferenceArray<E>(ceilingNextPowerOfTwo(estimatedCapacity));
        mask = array.length() - 1;
        head = new AtomicLong();
        tail = new AtomicLong();
        headNode = new Node<E>(null);
        tailNode = new AtomicReference<Node<E>>(headNode);
    }

    @Override
    public boolean isEmpty() {
        return head.get() == tail.get();
    }

    @Override
    public int size() {
        return (int) (tail.get() - head.get());
    }

    public int estimatedCapacity() {
        return array.length();
    }

    @Override
    public boolean offer(E e) {
        if (e == null) {
            throw new NullPointerException();
        }
        long t = tail.getAndIncrement();
        long h = head.get();
        if ((t - h) < array.length()) {
            int index = (int) (t & mask);
            array.lazySet(index, e);
        } else {
            Node<E> node = new Node<E>(e);
            tailNode.getAndSet(node).lazySet(node);
        }
        return true;
    }

    @Override
    public E peek() {
        long h = head.get();
        long t = tail.get();
        if (h == t) {
            return null;
        }
        int index = (int) h & mask;
        E e = array.get(index);
        return (e == null) ? headNode.get().value : e;
    }

    @Override
    public E poll() {
        long h = head.get();
        long t = tail.get();
        if (h == t) {
            return null;
        }
        int index = (int) h & mask;
        E e = array.get(index);
        if (e == null) {
            Node<E> next = headNode.get();
            headNode = next;
            e = next.value;
        } else {
            array.lazySet(index, null);
        }
        head.lazySet(h + 1);
        return e;
    }

    /**
     * Removes at most the given number of available elements from this queue and adds them to the
     * given array.
     *
     * @param out the array to transfer elements into
     * @return the number of elements transferred
     */
    public int drainTo(E[] out) {
        for (int i = 0; i < out.length; i++) {
            out[i] = poll(); // TODO(bmanes): optimize
            if (out[i] == null) {
                return i;
            }
        }
        return out.length;
    }

    @Override
    public Iterator<E> iterator() {
        return new Iterator<E>() {
            long cursor = head.get();
            Node<E> cursorNode = headNode;
            long expectedModCount = cursor;

            @Override
            public boolean hasNext() {
                return cursor != tail.get();
            }

            @Override
            public E next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                } else if (head.get() != expectedModCount) {
                    throw new ConcurrentModificationException();
                }
                E e;
                if ((cursor - expectedModCount) < array.length()) {
                    int index = (int) cursor & mask;
                    e = array.get(index);
                } else {
                    Node<E> next = cursorNode.get();
                    cursorNode = next;
                    e = next.value;
                }
                cursor++;
                return e;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    static final class Node<T> extends AtomicReference<Node<T>> {
        private static final long serialVersionUID = 1L;

        final T value;

        Node(T value) {
            this.value = value;
        }
    }
}