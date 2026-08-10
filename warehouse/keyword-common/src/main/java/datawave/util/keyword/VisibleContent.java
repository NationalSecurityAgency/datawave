package datawave.util.keyword;

import com.google.common.base.Objects;

/** A simple tuple that holds content and an associated visibility string */
public class VisibleContent {
    final String visibility;
    final String content;

    public VisibleContent(String visibility, String content) {
        if (visibility == null) {
            throw new NullPointerException("Visibility was null");
        }
        if (content == null) {
            throw new NullPointerException("Content was null");
        }
        this.visibility = visibility;
        this.content = content;
    }

    public String getContent() {
        return content;
    }

    public String getVisibility() {
        return visibility;
    }

    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass())
            return false;
        VisibleContent that = (VisibleContent) o;
        return Objects.equal(visibility, that.visibility) && Objects.equal(content, that.content);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(visibility, content);
    }
}
