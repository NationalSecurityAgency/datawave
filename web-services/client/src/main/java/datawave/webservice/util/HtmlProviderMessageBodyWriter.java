package datawave.webservice.util;

import java.lang.annotation.Annotation;
import java.lang.reflect.Type;

import javax.ws.rs.core.MediaType;

import datawave.webservice.HtmlProvider;

public class HtmlProviderMessageBodyWriter extends AbstractHtmlProviderMessageBodyWriter<HtmlProvider> {

    /*
     * (non-Javadoc)
     *
     * @see javax.ws.rs.ext.MessageBodyWriter#isWriteable(java.lang.Class, java.lang.reflect.Type, java.lang.annotation.Annotation[],
     * javax.ws.rs.core.MediaType)
     */
    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return HtmlProvider.class.isAssignableFrom(type);
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
