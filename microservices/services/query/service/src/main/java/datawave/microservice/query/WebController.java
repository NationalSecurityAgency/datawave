package datawave.microservice.query;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.info.GitProperties;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;

@Controller
public class WebController {
    
    @Autowired
    private GitProperties gitProperties;
    
    @GetMapping("/")
    public String index(Model model) {
        model.addAttribute("gitBuildUserEmail", gitProperties.get("build.user.email"));
        model.addAttribute("gitBranch", gitProperties.getBranch());
        model.addAttribute("gitCommitId", gitProperties.getCommitId());
        model.addAttribute("gitBuildTime", gitProperties.getInstant("build.time"));
        
        return "index";
    }
    
    @GetMapping("/query_help.html")
    public String query_help(Model model) {
        return "query_help";
    }
    
}
