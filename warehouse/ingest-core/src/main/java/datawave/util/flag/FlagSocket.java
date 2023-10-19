package datawave.util.flag;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;

import org.apache.log4j.Logger;

/**
 *
 */
public class FlagSocket implements Runnable, PropertyChangeListener {

    private static final Logger log = Logger.getLogger(FlagSocket.class);
    private ServerSocket serverSocket;
    private volatile boolean running = true;
    private PropertyChangeSupport support;

    public FlagSocket(int port) throws IOException {
        serverSocket = new ServerSocket(port);
    }

    @Override
    public void run() {
        // register ourselves to observe...
        support.addPropertyChangeListener(this);

        log.info("Listening for shutdown commands on port " + serverSocket.getLocalPort());
        while (running) {
            try {
                Socket s = serverSocket.accept();
                SocketAddress remoteAddress = s.getRemoteSocketAddress();
                try {
                    log.info(remoteAddress + " connected to the shutdown port");
                    s.setSoTimeout(30000);
                    InputStream is = s.getInputStream();
                    BufferedReader rdr = new BufferedReader(new InputStreamReader(is));
                    String line = rdr.readLine();
                    is.close();
                    s.close();
                    support.firePropertyChange("line", line, line);
                } catch (SocketTimeoutException e) {
                    log.info("Timed out waiting for input from " + remoteAddress);
                }
            } catch (SocketException e) {
                if (running) {
                    log.info("Socket Exception occurred: " + e.getMessage(), e);
                }
            } catch (IOException e) {
                log.error("Error waiting for shutdown connection: " + e.getMessage(), e);
            }
        }
    }

    @Override
    public void propertyChange(PropertyChangeEvent propertyChangeEvent) {
        String arg = propertyChangeEvent.getPropertyName();
        if ("shutdown".equals(arg)) {
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
