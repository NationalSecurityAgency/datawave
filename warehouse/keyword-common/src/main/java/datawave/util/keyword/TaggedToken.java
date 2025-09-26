package datawave.util.keyword;

import com.google.common.base.Objects;

/**
 * A data type used to collect intermediate information about tokens, their positions and type tags, ultimately converted to a YakeToken
 */
public final class TaggedToken {
    private final String token;
    private final int sentenceId;
    private final int position;
    private final String tag;

    // used so frequently that we cache this at creation time.
    private final String lowercaseToken;

    public TaggedToken(String token, int sentenceId, int position, String tag) {
        this.token = token;
        this.sentenceId = sentenceId;
        this.position = position;
        this.tag = tag;

        this.lowercaseToken = token.toLowerCase();
    }

    public String getToken() {
        return token;
    }

    public String getLowercaseToken() {
        return lowercaseToken;
    }

    public int getSentenceId() {
        return sentenceId;
    }

    public int getPosition() {
        return position;
    }

    public String getTag() {
        return tag;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        TaggedToken that = (TaggedToken) o;
        return sentenceId == that.sentenceId && position == that.position && Objects.equal(token, that.token) && Objects.equal(tag, that.tag);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(token, sentenceId, position, tag);
    }

    @Override
    public String toString() {
        return "['" + token + "', sentenceId=" + sentenceId + ", position=" + position + ", tag=" + tag + "]";
    }
}
