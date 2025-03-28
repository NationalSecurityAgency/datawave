package datawave.microservice.http.converter.html;

import org.springframework.lang.Nullable;

import datawave.microservice.config.web.DatawaveServerProperties;
import datawave.webservice.HtmlProvider;

/**
 * A {@link org.springframework.http.converter.HttpMessageConverter} that converts an {@link HtmlProvider} to HTML. This class does not support reading HTML and
 * converting to an {@link HtmlProvider}.
 */
public class HtmlProviderHttpMessageConverter extends AbstractDatawaveHttpMessageConverter<HtmlProvider> {
    
    public HtmlProviderHttpMessageConverter(DatawaveServerProperties datawaveServerProperties, @Nullable BannerProvider bannerProvider) {
        super(datawaveServerProperties, bannerProvider);
    }
    
    @Override
    protected boolean supports(Class<?> clazz) {
        return HtmlProvider.class.isAssignableFrom(clazz);
    }
    
    @Override
    public String getTitle(HtmlProvider htmlProvider) {
        return htmlProvider.getTitle();
    }
    
    @Override
    public String getHeadContent(HtmlProvider htmlProvider) {
        return htmlProvider.getHeadContent();
    }
    
    @Override
    public String getPageHeader(HtmlProvider htmlProvider) {
        return htmlProvider.getPageHeader();
    }
    
    @Override
    public String getMainContent(HtmlProvider htmlProvider) {
        return htmlProvider.getMainContent();
    }
}
