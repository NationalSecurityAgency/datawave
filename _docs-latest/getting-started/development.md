---
title: Development Overview
tags: [getting_started, development]
summary: This page provides a brief overview of development environment setup and configuration
---

## Prerequisites

Git, Java 11, Maven 3.x

## Clone the Repository

<a class="btn btn-success" style="width: 220px;" href="{{ site.repository_url }}/" role="button" target="_blank"><i class="fa fa-github fa-lg"></i> DataWave Repo</a>

## Build DataWave

See the [DW_SOURCE/BUILDME.md][dw_blob_buildme] file for basic build instructions

## Configuring DataWave for Deployment

To configure DataWave for a specific deployment environment, the build utilizes standard Maven *properties* and *resource
filtering* to perform interpolation of values within configuration files and scripts throughout the project. Moreover,
the build leverages a custom Maven plugin, described below, to manage this process.

### Maven Plugin: *read-properties*

The **read-properties** plugin source is in the [datawave-utils][dw_read_properties] repo and the jar should already be deployed to a public artifact repository.

The DataWave build will leverage the **read-properties** plugin to load Maven properties in *\*.properties* files from
predefined locations. Moreover, the specific *\*.properties* files for a given deployment environment are typically
specified and activated by a distinct Maven profile for that environment.

The **read-properties** and **dev** profile snippets below from [DW_SOURCE/pom.xml][dw_blob_root_pom] demonstrate the setup...
```xml
...
<pluginManagement>
   <plugins>
      ...
      <plugin>
         <groupId>datawave</groupId>
         <artifactId>read-properties</artifactId>
         <version>2.0.1</version>
         <configuration>
            <quiet>true</quiet>

            <!-- Load *.properties files from these directories, in the order specified -->
            <directories combine.children="append">
              <!-- Starting with this dir (for defaults) -->
              <file>${datawave.root}/properties</file>
              <!-- Followed by this dir (for overrides) -->
              <file>${user.home}/.m2/datawave/properties</file>
            </directories>

            <!-- Read in these files, in the order specified -->
            <files combine.children="append">
              <file>default.properties</file>
            </files>

         </configuration>
         <executions>
            <execution>
               <id>read-properties</id>
               <goals>
                  <goal>read-properties</goal>
               </goals>
               <phase>validate</phase>
            </execution>
         </executions>
      </plugin>
      ...
   </plugins>
</pluginManagement>
...
<plugins>
   ...
   <!-- read-properties is activated in all modules -->
   <plugin>
      <groupId>datawave</groupId>
      <artifactId>read-properties</artifactId>
   </plugin>
   ...
</plugins>
...
<profiles>
   ...
   <profile>
      <!-- Profile for local/standalone environment -->
      <id>dev</id>
      ...
      <build>
         <pluginManagement>
            <plugins>
               <plugin>
                  <groupId>datawave</groupId>
                  <artifactId>read-properties</artifactId>
                  <configuration>
                     <!--
                       - Load these file names from any configured dirs, in the specified order,
                         and in addition to any files already configured (e.g., default.properties)

                       - Password files should only be saved to ~/.m2/datawave/properties/,
                         i.e., don't commit them to repo
                     -->
                     <files>
                        <file>dev.properties</file>
                        <file>dev-passwords.properties</file>
                     </files>
                  </configuration>
               </plugin>
            </plugins>
         </pluginManagement>
      </build>
   </profile>
   ...
</profiles>
...
```

### Custom Configurations

To customize the configuration for a specific deployment environment, simply set the desired property overrides in your
local **~/.m2/datawave/properties/\*.properties** file(s) for the Maven profile in question. Or create a new Maven profile
along with a new set of \*.properties files for the targeted environment.

## Required Properties

### Maven Plugin: *assert-properties*

The **assert-properties** plugin source is in the [datawave-utils][dw_assert_properties] repo and the jar should already be deployed to a public artifact repository.

The **assert-properties** plugin is configured on a per-module basis to define the set of *required* Maven
properties for that module. Thus, if any required properties are not set, then the module build will fail with a
descriptive message. See [DW_SOURCE/web-services/pom.xml][dw_blob_web_services_pom] for example usage.

[dw_blob_web_services_pom]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/web-services/pom.xml#L694
[dw_assert_properties]: https://github.com/NationalSecurityAgency/datawave-utils/tree/main/assert-properties
[dw_read_properties]: https://github.com/NationalSecurityAgency/datawave-utils/tree/main/read-properties
[dw_blob_root_pom]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/pom.xml
[dw_blob_buildme]: https://github.com/NationalSecurityAgency/datawave/blob/{{ page.release_tag }}/BUILDME.md
