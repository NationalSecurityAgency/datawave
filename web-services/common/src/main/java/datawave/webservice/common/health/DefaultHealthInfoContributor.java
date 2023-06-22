package datawave.webservice.common.health;

import datawave.webservice.common.health.HealthBean.BuildInfo;
import datawave.webservice.common.health.HealthBean.CommitInfo;
import datawave.webservice.common.health.HealthBean.GitInfo;
import datawave.webservice.common.health.HealthBean.VersionInfo;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.PropertiesLoaderUtils;

import java.io.IOException;
import java.util.Properties;

public class DefaultHealthInfoContributor implements HealthInfoContributor {
    @Override
    public VersionInfo versionInfo() {
        String buildTime = null;
        GitInfo gitInfo = null;
        try {
            ClassPathResource cpr = new ClassPathResource("/git.properties", getClass());
            Properties gitProps = PropertiesLoaderUtils.loadProperties(cpr);
            CommitInfo ci = new CommitInfo(gitProps.getProperty("git.commit.time"), gitProps.getProperty("git.commit.id.abbrev"));
            gitInfo = new GitInfo(ci, gitProps.getProperty("git.branch"));
            buildTime = gitProps.getProperty("git.build.time");
        } catch (IOException e) {
            // Ignore -- we just won't have git info.
        }

        String version = getClass().getPackage().getImplementationVersion();
        BuildInfo buildInfo = new BuildInfo(version, buildTime);
        return new VersionInfo("default", buildInfo, gitInfo);
    }
}
