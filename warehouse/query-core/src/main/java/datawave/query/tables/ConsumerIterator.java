package datawave.query.tables;

import java.util.Iterator;

public class ConsumerIterator<T> implements Iterator<T> {
    private final ProducerConsumerBuffer<T> queue;
    private T next;

    public ConsumerIterator(ProducerConsumerBuffer<T> queue) {
        this.queue = queue;
    }

    /**
     * Fetches the next item for the iterator to return if not already fetched, blocking until available or closed. When fetching from the queue, the queue will
     * be impacted by this call
     *
     * @return true if there is another item available from the queue, false otherwise
     */
    @Override
    public boolean hasNext() {
        if (next == null) {
            try {
                next = queue.take();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        return next != null;
    }

    /**
     *
     * @return the next value fetch from hasNext
     */
    @Override
    public T next() {
        T toReturn = next;
        next = null;
        return toReturn;
    }

    public static void main(String[] args) {
        ProducerConsumerBuffer<Integer> queue = new ProducerConsumerBuffer<>(25);

        Thread producer = new Thread(() -> {
            try {
                for (int i = 0; i < 1000; i++) {
                    queue.put(i);
                }
                System.out.println("done producing");
                queue.close();
                System.out.println("done");
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                e.printStackTrace();
            }
        });

        Thread consumer = new Thread(() -> {
            ConsumerIterator<Integer> iterator = new ConsumerIterator<>(queue);
            while (iterator.hasNext()) {
                Integer i = iterator.next();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                System.out.println(i);
            }
        });

        producer.start();
        consumer.start();
    }
}
