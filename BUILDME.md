# Building Datawave

## Generating a Github Repository access token

In order to download datawave artifacts from the github package repository, you will need to set credentials in 
your maven `settings.xml` file.

You should first create a classic personal access token on github [here](https://github.com/settings/tokens). Be 
sure to give the token at least the following permissions:
 * `read:packages`

Save the token value, and create a server entry for the github package repo in your maven `settings.xml` file, like so:
```xml
   <servers>
      <server>
         <id>github-datawave</id>
         <username>PUT_YOUR_GITHUB_USERNAME_HERE</username>
         <password>PUT_YOUR_PERSONAL_ACCESS_TOKEN_HERE</password>
      </server>
   </servers>
```
The id of the server matters, and should match what is used in the datawave parent pom.

## Building Datawave

To perform a full (non-release) 'dev' build  without unit tests:

```bash
mvn -Pdev -Ddeploy -Dtar -DskipTests clean install
```

This command will produce the following deployment archives:

1. Web Service: `./web-services/deploy/application/target/datawave-ws-deploy-application-${project.version}-dev.tar.gz`
2. Ingest: `./warehouse/assemble/datawave/target/datawave-dev-${project.version}-dist.tar.gz`

### Building A Release

In order to build a release, you must also define the dist variable by adding `-Ddist` to the command-line as follows:

```bash
mvn -Pdev,examples -Ddeploy -Dtar -Ddist -DskipTests clean install
```

### Building a Docker web image

In order to build a Docker container for the web services, you can run with the following maven profiles: `-Pdeploy-ws,docker`

```bash
mvn clean package -Pdev,assemble,deploy-ws -Pdocker -DskipTests  
```

Note that this will build javadocs and source jars.

### Building an RPM

To build the RPM specify both the assemble and rpm profiles should be specified, as follows:

```bash
mvn -Pdev,assemble,rpm -Ddeploy -Dtar -Ddist -DskipTests clean install
```

# Building Microservices

Datawave web services utilize several microservices at runtime (currently authorization and auditing, although that
list will expand soon). Datawave depends on api modules for some of these services, and the dependencies are set in
the parent pom (see `version.datawave.*` properties) to released versions. If you wish to build the microservices
for some reason, you can simply add `-Dservices` to your maven build command.  If you wish to build the starters you can add `-Dstarters` and for the utility modules add `-Dutils`.

### Releasing Microservices

Each subdirectory under the `services` folder is treated as a separate project. Therefore if you wish to build a
release for any of the services (or their APIs), change directory to the appropriate service and build and deploy
the release with `mvn -Ddist clean deploy`. We are currently deploying our artifacts to the github package repo.
Therefore, to execute the deployment, you will need to set credentials in your maven `settings.xml` file.
You should first create a classic personal access token on github [here](https://github.com/settings/tokens). Be 
sure to give the token at least the following permissions:
 * `write:packages`
 * `delete:packages`

Save the token value, and create a server entry for the github package repo in your maven `settings.xml` file, like so:
```xml
<servers>
      <server>
         <id>github-datawave</id>
         <username>PUT_YOUR_GITHUB_USERNAME_HERE</username>
         <password>PUT_YOUR_PERSONAL_ACCESS_TOKEN_HERE</password>
      </server>
   </servers>
```
The id of the server matters, and should match what is used in the datawave parent pom.

Releases for individual services are generally tagged using the pattern `svc_<directory_name>_<version>`. For example,
the authorization service API version 1.0 is tagged with `svc_authorization-api_1.0`.

Note that simply building a new API or service release won't ensure that it is used anywhere. You will need to update
build properties in either the datawave parent pom or within other service poms (for cross-service dependencies) to
ensure that the new version is used. Look for properties starting with `version.datawave.` to see what to update.
If you are updating an API module, you should be careful. In general, the associated service will need to be updated as
well to support the API changes. The service should _add_ a new version of the API and continue to support the old
version until it can be ensured that there are no more consumers of the old API.

# Troubleshooting Build Issues

Due to our use of the **git-commit-id-plugin** Maven plugin, your build could fail with...
```
....
[INFO] ------------------------------------------------------------------------
[INFO] BUILD FAILURE
[INFO] ------------------------------------------------------------------------
...
[ERROR] Failed to execute goal pl.project13.maven:git-commit-id-plugin:2.2.4:revision (default) on project XXXXXXX:
.git directory is not found! Please specify a valid [dotGitDirectory] in your pom.xml
```
...under the following circumstances:

* You've downloaded the source archive (zip) from github.com rather than pulling it via `git clone ...`
* Or your local `.git` directory has become corrupted for some reason

### Resolution
* Use `git clone ...` to retrieve the source code into a new local directory and then retry the build
* If `git clone` is not an option for some reason, either edit POMs to disable the `git-commit-id-plugin` or `git init` a new local repo in the directory containing the source code and make at least one commit. This option is not recommended and only intended as a last resort, as you will be prevented from submitting pull requests and performing other Git workflows associated with this repository
