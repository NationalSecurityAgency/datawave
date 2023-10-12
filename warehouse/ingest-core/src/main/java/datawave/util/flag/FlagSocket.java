package datawave.util.flag;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.concurrent.LinkedBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class FlagSocket implements Runnable {

    private static final Logger log = LoggerFactory.getLogger(FlagSocket.class);
    private final ServerSocket serverSocket;
    private volatile boolean running = true;
    private final LinkedBlockingQueue<String> socketMessageQueue;

    public FlagSocket(int port, LinkedBlockingQueue<String> socketMessageQueue) throws IOException {
        this.serverSocket = new ServerSocket(port);
        this.socketMessageQueue = socketMessageQueue;
    }

    @Override
    public void run() {
        log.info("Listening for shutdown commands on port {}", serverSocket.getLocalPort());
        while (running) {
            try {
                Socket s = serverSocket.accept();
                SocketAddress remoteAddress = s.getRemoteSocketAddress();
                try {
                    log.info("{} connected to the shutdown port", remoteAddress);
                    s.setSoTimeout(30000);
                    InputStream is = s.getInputStream();
                    BufferedReader rdr = new BufferedReader(new InputStreamReader(is));
                    String line = rdr.readLine();
                    is.close();
                    s.close();
                    log.info("Add message to queue: {}", line);
                    if (null != line) {
                        socketMessageQueue.add(line);
                        checkFoActionableMessages(line);
                    }
                } catch (SocketTimeoutException e) {
                    log.info("Timed out waiting for input from {}", remoteAddress);
                }
            } catch (SocketException e) {
                if (running) {
                    log.info("Socket Exception occurred: {}", e.getMessage(), e);
                }
            } catch (IOException e) {
                log.error("Error waiting for shutdown connection: {}", e.getMessage(), e);
            }
        }
    }

    private void checkFoActionableMessages(String message) {
        if ("shutdown".equals(message)) {
            log.info("Shutdown call received. Socket exiting.");
            running = false;
            try {
                serverSocket.close();
            } catch (IOException ex) {
                log.info("Failed to close server socket on shutdown", ex);
            }
        }
    }
}
