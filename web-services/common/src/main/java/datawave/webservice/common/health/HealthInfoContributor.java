package datawave.webservice.common.health;

import datawave.webservice.common.health.HealthBean.VersionInfo;

public interface HealthInfoContributor {
    VersionInfo versionInfo();
}
