package datawave.microservice.dictionary;

import org.springframework.http.MediaType;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import com.codahale.metrics.annotation.Timed;

@Controller
@RequestMapping(path = "/data/v2", produces = {MediaType.TEXT_HTML_VALUE})
public class HTMLControllerV2 {
    @GetMapping("/")
    @Timed(name = "dw.dictionary.data.get", absolute = true)
    public String get() {
        return "/data/v2/index.html";
    }
}
