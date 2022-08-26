package datawave.query.statsd;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created on 4/27/17.
 */
public class QueryStatsdDClientTest {
    
    @Test
    public void testMetrics() {
        SimpleUDPServer server;
        server = new SimpleUDPServer(9875);
        server.start();
        
        // create a client
        QueryStatsDClient client = new QueryStatsDClient("queryid", "localhost", server.getPort(), 50);
        
        // send some metrics
        client.seek();
        client.next();
        client.next();
        client.addSource();
        client.addSource();
        client.addSource();
        client.timing("MyMethod", 4242);
        client.flush();
        
        // wait at least 200 ms to ensure the data is received
        sleep();
        
        // verify the sent metrics
        Set<String> messages = new HashSet<>(server.getMessages());
        System.out.println(messages);
        Assertions.assertFalse(messages.isEmpty(), "Did not receive messages");
        Assertions.assertEquals(4, messages.size(), "Did not receive 4 messages");
        Assertions.assertTrue(messages.contains("queryid.dwquery.seek_calls:1|c"), "Did not receive correct seek message");
        Assertions.assertTrue(messages.contains("queryid.dwquery.next_calls:2|c"), "Did not receive correct next message");
        Assertions.assertTrue(messages.contains("queryid.dwquery.sources:3|c"), "Did not receive correct sources message");
        Assertions.assertTrue(messages.contains("queryid.dwquery.MyMethod:4242|c"), "Did not receive correct timing message");
        
        client.stop();
        server.stop();
    }
    
    @Test
    public void testMultipleQueryMetrics() {
        SimpleUDPServer server;
        server = new SimpleUDPServer(9876);
        server.start();
        
        // create a client or two
        QueryStatsDClient client1 = new QueryStatsDClient("queryid1", "localhost", server.getPort(), 50);
        QueryStatsDClient client2 = new QueryStatsDClient("queryid2", "localhost", server.getPort(), 50);
        
        // send some metrics
        client1.seek();
        client2.seek();
        client2.seek();
        client1.next();
        client1.next();
        client2.next();
        client1.addSource();
        client1.addSource();
        client1.addSource();
        client2.addSource();
        client1.timing("MyMethod", 4242);
        client2.timing("MyMethod", 4243);
        client1.flush();
        client2.flush();
        
        // wait at least 200 ms to ensure the data is received
        sleep();
        
        // verify the sent metrics
        Set<String> messages = new HashSet<>(server.getMessages());
        System.out.println(messages);
        Assertions.assertFalse(messages.isEmpty(), "Did not receive messages");
        Assertions.assertEquals(8, messages.size(), "Did not receive 8 messages");
        Assertions.assertTrue(messages.contains("queryid1.dwquery.seek_calls:1|c"), "Did not receive correct seek message");
        Assertions.assertTrue(messages.contains("queryid1.dwquery.next_calls:2|c"), "Did not receive correct next message");
        Assertions.assertTrue(messages.contains("queryid1.dwquery.sources:3|c"), "Did not receive correct sources message");
        Assertions.assertTrue(messages.contains("queryid1.dwquery.MyMethod:4242|c"), "Did not receive correct timing message");
        
        Assertions.assertTrue(messages.contains("queryid2.dwquery.seek_calls:2|c"), "Did not receive correct seek message");
        Assertions.assertTrue(messages.contains("queryid2.dwquery.next_calls:1|c"), "Did not receive correct next message");
        Assertions.assertTrue(messages.contains("queryid2.dwquery.sources:1|c"), "Did not receive correct sources message");
        Assertions.assertTrue(messages.contains("queryid2.dwquery.MyMethod:4243|c"), "Did not receive correct timing message");
        
        client1.stop();
        client2.stop();
        server.stop();
    }
    
    public static class SimpleUDPServer implements Runnable {
        
        private final List<String> messages = new ArrayList<>();
        private Thread thread = null;
        private boolean running = false;
        private boolean stop = false;
        private volatile int port;
        private IOException exception = null;
        
        SimpleUDPServer(int port) {
            this.port = port;
        }
        
        public void start() {
            stop = false;
            messages.clear();
            
            do {
                exception = null;
                port++;
                thread = new Thread(this, "SimpleUDPServer on port " + port);
                thread.start();
                
                // wait until running or failed to open port
                while (!running && exception == null) {
                    try {
                        Thread.sleep(1);
                    } catch (Exception e) {
                        // ok
                    }
                }
            } while (!running);
        }
        
        public void stop() {
            stop = true;
            thread.interrupt();
            thread = null;
        }
        
        public List<String> getMessages() {
            return messages;
        }
        
        public int getPort() {
            return port;
        }
        
        public void run() {
            try {
                DatagramSocket serverSocket = new DatagramSocket(port);
                running = true;
                while (!stop) {
                    byte[] receiveData = new byte[1024];
                    DatagramPacket receivePacket = new DatagramPacket(receiveData, receiveData.length);
                    serverSocket.receive(receivePacket);
                    int len = -1;
                    for (int i = 0; len == -1 && i < receiveData.length; i++) {
                        if (receiveData[i] == '\0') {
                            len = i;
                        }
                    }
                    String sentence = new String(receiveData, 0, (len == -1 ? receiveData.length : len));
                    messages.add(sentence);
                }
            } catch (IOException ioe) {
                exception = ioe;
            }
            running = false;
        }
    }
    
    private void sleep() {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < (long) 1000) {
            try {
                Thread.sleep((long) 1000 - Math.max(1, System.currentTimeMillis() - start));
            } catch (InterruptedException ie) {
                // cont
            }
        }
    }
    
}
