package datawave.microservice.configcheck;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.Banner;
import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;

public class ConfigCheckApplication implements ApplicationRunner, ExitCodeGenerator {
    private static Logger log = LoggerFactory.getLogger(ConfigCheckApplication.class);
    
    private int exitCode;
    
    public static void main(String[] args) {
        SpringApplication app = new SpringApplication(ConfigCheckApplication.class);
        app.setBannerMode(Banner.Mode.OFF);
        System.exit(SpringApplication.exit(app.run(args)));
    }
    
    @Override
    public void run(ApplicationArguments args) throws Exception {
        log.info("Executing: application runner");
        
        log.info("Raw args:");
        for (int i = 0; i < args.getSourceArgs().length; i++) {
            log.info("args[{}]: {}", i, args.getSourceArgs()[i]);
        }
        
        log.info("Non-option args:");
        for (String arg : args.getNonOptionArgs()) {
            log.info("  " + arg);
        }
        
        log.info("Option args:");
        for (String name : args.getOptionNames()) {
            log.info("  " + name + "=" + String.join(",", args.getOptionValues(name)));
        }
        
        Output output = new CommandRunner(args).run();
        if (!output.isError()) {
            System.out.println(output.getMessage());
        } else {
            System.err.println(output.getMessage());
            this.exitCode = 1;
        }
    }
    
    @Override
    public int getExitCode() {
        return this.exitCode;
    }
}
