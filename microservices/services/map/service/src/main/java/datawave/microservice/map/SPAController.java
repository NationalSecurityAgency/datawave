package datawave.microservice.map;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;

import org.springframework.stereotype.Controller;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;

/**
 * Single Page Application Controller which is used to map requests to the single page application (index.html)
 */
@Controller
public class SPAController {
    // a request to any path that does not contain a period (and is not explicitly mapped already) will be forwarded to the root page (index.html)
    @GetMapping("/")
    public String get(@RequestParam MultiValueMap<String,String> parameters, HttpServletResponse response) {
        String queryId = parameters.getFirst("queryId");
        
        if (queryId != null) {
            Cookie cookie = new Cookie("queryId", queryId);
            cookie.setMaxAge(0);
            cookie.setPath("/");
            response.addCookie(cookie);
        }
        
        return "index.html";
    }
}
