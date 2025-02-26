package datawave.microservice.config.server;

import org.eclipse.jgit.transport.JschConfigSessionFactory;
import org.eclipse.jgit.transport.OpenSshConfig;
import org.eclipse.jgit.transport.SshSessionFactory;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.config.environment.Environment;
import org.springframework.cloud.config.server.environment.MultipleJGitEnvironmentProperties;
import org.springframework.cloud.config.server.environment.MultipleJGitEnvironmentRepository;
import org.springframework.core.env.ConfigurableEnvironment;

import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;

/**
 * An extension of {@link MultipleJGitEnvironmentRepository} that sets up JGit to handle encrypted private keys for git.
 */
@EnableConfigurationProperties(DatawaveJGitEnvironmentRepository.class)
@ConfigurationProperties("spring.cloud.config.server.git")
public class DatawaveJGitEnvironmentRepository extends MultipleJGitEnvironmentRepository {
    private boolean initialized = false;
    
    public DatawaveJGitEnvironmentRepository(ConfigurableEnvironment environment, MultipleJGitEnvironmentProperties properties) {
        super(environment, properties);
    }
    
    @Override
    public Environment findOne(String application, String profile, String label) {
        if (!initialized && getPassword() != null) {
            SshSessionFactory.setInstance(new JschConfigSessionFactory() {
                @Override
                protected void configure(OpenSshConfig.Host hc, Session session) {
                    session.setUserInfo(new KeyPassphraseUserInfo(getPassword()));
                }
            });
            initialized = true;
        }
        return super.findOne(application, profile, label);
    }
    
    private static class KeyPassphraseUserInfo implements UserInfo {
        private String passphrase;
        
        public KeyPassphraseUserInfo(String passphrase) {
            this.passphrase = passphrase;
        }
        
        @Override
        public String getPassphrase() {
            return passphrase;
        }
        
        @Override
        public String getPassword() {
            return null;
        }
        
        @Override
        public boolean promptPassword(String message) {
            return false;
        }
        
        @Override
        public boolean promptPassphrase(String message) {
            return passphrase != null;
        }
        
        @Override
        public boolean promptYesNo(String message) {
            return false;
        }
        
        @Override
        public void showMessage(String message) {
            // empty on purpose
        }
    }
}
