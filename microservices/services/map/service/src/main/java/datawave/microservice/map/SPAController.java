package datawave.microservice.map;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

/**
 * Single Page Application Controller which is used to map requests to the single page application (index.html)
 */
@Controller
public class SPAController {
    // a request to any path that does not contain a period (and is not explicitly mapped already) will be forwarded to the root page (index.html)
    @GetMapping("/**/{path:[^\\.]*}")
    public String forward(@PathVariable String path) {
        System.out.println("FORWARDING! " + path);
        return "forward:/";
    }
}
