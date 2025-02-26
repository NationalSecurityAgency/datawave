package datawave.microservice.http.converter.html;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.IOException;
import java.util.Collections;

import org.springframework.http.HttpInputMessage;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.MediaType;
import org.springframework.http.converter.AbstractHttpMessageConverter;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.lang.Nullable;

import datawave.microservice.config.web.DatawaveServerProperties;

abstract public class AbstractDatawaveHttpMessageConverter<T> extends AbstractHttpMessageConverter<T> {
    protected final DatawaveServerProperties datawaveServerProperties;
    @Nullable
    protected final BannerProvider bannerProvider;
    
    public AbstractDatawaveHttpMessageConverter(DatawaveServerProperties datawaveServerProperties, @Nullable BannerProvider bannerProvider) {
        this.datawaveServerProperties = datawaveServerProperties;
        this.bannerProvider = bannerProvider;
        setSupportedMediaTypes(Collections.singletonList(MediaType.TEXT_HTML));
    }
    
    @Override
    protected boolean canRead(@Nullable MediaType mediaType) {
        return false;
    }
    
    @Override
    @Nullable
    @SuppressWarnings("ConstantConditions")
    protected T readInternal(Class<? extends T> clazz, HttpInputMessage inputMessage) throws HttpMessageNotReadableException {
        return null;
    }
    
    @Override
    protected void writeInternal(T t, HttpOutputMessage outputMessage) throws IOException, HttpMessageNotWritableException {
        outputMessage.getBody().write(createHtml(t));
    }
    
    public abstract String getTitle(T t);
    
    public abstract String getHeadContent(T t);
    
    public abstract String getPageHeader(T t);
    
    public abstract String getMainContent(T t);
    
    protected byte[] createHtml(T t) {
        
        String headBanner = (bannerProvider != null) ? bannerProvider.getHeadBanner() : "";
        String footBanner = (bannerProvider != null) ? bannerProvider.getFootBanner() : "";
        
        //@formatter:off
        String builder = "<html>" +
                "<head>" +
                    "<meta http-equiv=\"content-type\" content=\"text/html; charset=UTF-8\"/>" +
                    "<title>DATAWAVE - " + getTitle(t) + "</title>" +
                    "<link rel='stylesheet' type='text/css' href='" + datawaveServerProperties.getCdnUri() + "css/screen.css' media='screen' />" +
                    getHeadContent(t) +
                "</head>" +
                "<body>" +
                    headBanner +
                    "<h1>" + getPageHeader(t) + "</h1>" +
                    "<div>" + getMainContent(t) + "</div>" +
                    "<br/>" +
                    footBanner +
                "</body>" +
                "</html>\n";
        //@formatter:on
        return builder.getBytes(UTF_8);
    }
}
