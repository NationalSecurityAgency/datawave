package datawave.common.util.concurrent;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class BoundedBlockingQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {
    private final Queue<E> queue;

    private final int maxCapacity;

    final ReentrantLock lock;
    private final Condition notEmpty;
    private final Condition notFull;

    public BoundedBlockingQueue(int maxCapacity, Queue<E> queue) {
        this(maxCapacity, queue, false);
    }

    public BoundedBlockingQueue(int maxCapacity, Queue<E> queue, boolean fair) {
        this.maxCapacity = maxCapacity;
        this.queue = queue;
        this.lock = new ReentrantLock(fair);
        this.notEmpty = this.lock.newCondition();
        this.notFull = this.lock.newCondition();
    }

    @Override
    public boolean add(E e) {
        return super.add(e);
    }

    @Override
    public boolean offer(E e) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (queue.size() == maxCapacity)
                return false;
            else {
                boolean ret = queue.offer(e);
                notEmpty.signal();
                return ret;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void put(E e) throws InterruptedException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (queue.size() == maxCapacity)
                notFull.await();
            queue.offer(e);
            notEmpty.signal();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (queue.size() == maxCapacity) {
                if (nanos <= 0)
                    return false;
                nanos = notFull.awaitNanos(nanos);
            }
            boolean ret = queue.offer(e);
            notEmpty.signal();
            return ret;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (queue.isEmpty())
                return null;
            else {
                E e = queue.poll();
                notFull.signal();
                return e;
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E peek() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return queue.peek();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E take() throws InterruptedException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (queue.isEmpty())
                notEmpty.await();
            E e = queue.poll();
            notFull.signal();
            return e;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public E poll(long timeout, TimeUnit unit) throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (queue.isEmpty()) {
                if (nanos <= 0)
                    return null;
                nanos = notEmpty.awaitNanos(nanos);
            }
            E e = queue.poll();
            notFull.signal();
            return e;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Iterator<E> iterator() {
        return queue.iterator();
    }

    @Override
    public int size() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return queue.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int remainingCapacity() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return maxCapacity - queue.size();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean remove(Object o) {
        if (o == null)
            return false;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (!queue.isEmpty()) {
                boolean ret = queue.remove(o);
                notFull.signal();
                return ret;
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean contains(Object o) {
        if (o == null)
            return false;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (!queue.isEmpty()) {
                return queue.contains(o);
            }
            return false;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int drainTo(Collection<? super E> c) {
        return drainTo(c, Integer.MAX_VALUE);
    }

    @Override
    public int drainTo(Collection<? super E> c, int maxElements) {
        if (c == this)
            throw new IllegalArgumentException();
        if (maxElements <= 0)
            return 0;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            int n = Math.min(maxElements, queue.size());
            int i = 0;
            try {
                while (i < n) {
                    E x = queue.poll();
                    c.add(x);
                    i++;
                }
                return n;
            } finally {
                for (; i > 0 && lock.hasWaiters(notFull); i--)
                    notFull.signal();
            }
        } finally {
            lock.unlock();
        }
    }
}
