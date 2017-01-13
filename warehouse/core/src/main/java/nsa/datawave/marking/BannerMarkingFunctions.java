package nsa.datawave.marking;

import java.util.Map;
import java.util.Set;

/**
 * Indicates that a {@link MarkingFunctions} is capable of supplying a "banner" form of security marking. This is a form that is often used to place a banner at
 * the header or footer of a web page or document.
 */
public interface BannerMarkingFunctions extends MarkingFunctions {
    String getSecurityBanner(Map<String,String> markings) throws MarkingFunctions.Exception;
    
    void setUndisplayedVisibilities(Set<String> undisplayedVisibilities);
}
