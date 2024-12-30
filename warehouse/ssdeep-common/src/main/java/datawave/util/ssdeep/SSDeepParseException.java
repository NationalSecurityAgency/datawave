package datawave.util.ssdeep;

import java.nio.charset.StandardCharsets;

public class SSDeepParseException extends RuntimeException {
    final String message;
    final String input;

    public SSDeepParseException(String message, byte[] input) {
        this.message = message;
        this.input = new String(input, StandardCharsets.UTF_8);
    }

    public SSDeepParseException(String message, String input) {
        this.message = message;
        this.input = input;
    }

    @Override
    public String toString() {
        return "SSDeepParseException{" + "message='" + message + '\'' + ", input='" + input + '\'' + '}';
    }
}
