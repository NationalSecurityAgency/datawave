package datawave.util.keyword;

import com.google.common.base.Objects;

/** A simple two item tuple used for associating an integer value with a token. */
public final class TokenValue {
    private final String token;
    private final int value;

    // used so frequently that we cache it at creation time.
    private final String lowercaseToken;

    public TokenValue(String token, int count) {
        this.token = token;
        this.value = count;

        this.lowercaseToken = token.toLowerCase();
    }

    public String getToken() {
        return token;
    }

    public String getLowercaseToken() {
        return lowercaseToken;
    }

    public int getValue() {
        return value;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        TokenValue that = (TokenValue) o;
        return value == that.value && Objects.equal(token, that.token);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(token, value);
    }

    @Override
    public String toString() {
        return "['" + token + "', value=" + value + "]";
    }
}
