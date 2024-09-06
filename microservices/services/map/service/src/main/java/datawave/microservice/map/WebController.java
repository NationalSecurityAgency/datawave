package datawave.microservice.map;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;

/**
 * Single Page Application Controller which is used to map requests to the single page application (index.html)
 */
@Controller
public class WebController {
    // a request to any path that does not contain a period (and is not explicitly mapped already) will be forwarded to the root page (index.html)
    @GetMapping("/")
    public String get() {
        return "index.html";
    }
}
