package datawave.metrics;

import java.nio.charset.Charset;
import java.text.NumberFormat;
import java.util.Locale;

import com.timgroup.statsd.NonBlockingUdpSender;
import com.timgroup.statsd.StatsDClientErrorHandler;
import com.timgroup.statsd.StatsDClientException;

/**
 * A client for sending metrics to StatsD. This makes use of {@link NonBlockingUdpSender} to send data to StatsD in a non-blocking fashion.
 */
class StatsDClient {
    
    private static final Charset STATS_D_ENCODING = Charset.forName("UTF-8");
    
    private static final StatsDClientErrorHandler NO_OP_HANDLER = e -> { /* No-op */};
    
    private final String prefix;
    private final NonBlockingUdpSender sender;
    
    private ThreadLocal<NumberFormat> numberFormat = ThreadLocal.withInitial(() -> {
        NumberFormat formatter = NumberFormat.getInstance(Locale.US);
        formatter.setGroupingUsed(false);
        formatter.setMaximumFractionDigits(19);
        return formatter;
    });
    
    public StatsDClient(String prefix, String hostname, int port) throws StatsDClientException {
        this(prefix, hostname, port, NO_OP_HANDLER);
    }
    
    public StatsDClient(String prefix, String hostname, int port, StatsDClientErrorHandler errorHandler) throws StatsDClientException {
        this.prefix = (prefix == null || prefix.trim().isEmpty()) ? "" : (prefix.trim() + ".");
        
        try {
            this.sender = new NonBlockingUdpSender(hostname, port, STATS_D_ENCODING, errorHandler);
        } catch (Exception e) {
            throw new StatsDClientException("Failed to start StatsD client", e);
        }
    }
    
    public void stop() {
        sender.stop();
    }
    
    public void count(String aspect, long delta) {
        send(messageFor(aspect, Long.toString(delta), "c", 1.0));
    }
    
    public void count(String aspect, double delta) {
        send(messageFor(aspect, stringValueOf(delta), "c", 1.0));
    }
    
    public void gauge(String aspect, long value) {
        gauge(aspect, Long.toString(value), value < 0);
    }
    
    public void gauge(String aspect, double value) {
        gauge(aspect, stringValueOf(value), value < 0);
    }
    
    public void gauge(String aspect, String value) {
        gauge(aspect, value, false);
    }
    
    private void gauge(String aspect, String value, boolean negative) {
        final StringBuilder message = new StringBuilder();
        if (negative) {
            message.append(messageFor(aspect, "0", "g")).append('\n');
        }
        message.append(messageFor(aspect, value, "g"));
        send(message.toString());
    }
    
    public void time(String aspect, long timeInMs) {
        send(messageFor(aspect, Long.toString(timeInMs), "ms", 1.0));
    }
    
    public void time(String aspect, double timeInMs) {
        send(messageFor(aspect, stringValueOf(timeInMs), "ms", 1.0));
    }
    
    private String messageFor(String aspect, String value, String type) {
        return messageFor(aspect, value, type, 1.0);
    }
    
    private String messageFor(String aspect, String value, String type, double sampleRate) {
        final String message = prefix + aspect + ':' + value + '|' + type;
        return (sampleRate == 1.0) ? message : (message + "|@" + stringValueOf(sampleRate));
    }
    
    private void send(final String message) {
        sender.send(message);
    }
    
    private String stringValueOf(double value) {
        return numberFormat.get().format(value);
    }
}
