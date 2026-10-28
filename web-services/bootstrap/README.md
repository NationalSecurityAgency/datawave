# Webservice configuration bootstrap

This non-web Spring Boot command fetches a Spring Cloud Config Server environment
and exports selected properties (or all properties) in Java `.properties` format.

Build from the repository root:

```sh
mvn -pl web-services/bootstrap -am package -DskipTests
```

Run the executable jar:

```sh
java -jar web-services/bootstrap/target/ws-bootstrap-8.5.0-SNAPSHOT.jar \
  --config-server-url=http://localhost:8888/configsaerver \
  --application-name=wildfly \
  --active-profiles=dev,local \
  --prefix=datawave.security.elytron \
  --output-file=/absolute/path/to/elytron.properties
```

`--config-server-url` is required, with exactly one nonblank value.
`--application-name`, `--active-profiles`, and `--prefix` are optional
and accept an empty value. Omitted or blank application names default to
`wildfly`. Omitted or blank profiles use the Config
Server `default` profile. The command
requests `{config-server-url}/{application-name}/{active-profiles}`; the server
controls profile precedence. URLs without a path (or with only `/`) default to
the `/configsaerver` context path; an explicit context path is preserved.

Without a prefix, all remote properties are exported with their original keys.
With a prefix, only keys starting with that prefix followed by `.` are exported.
The prefix and separator dot are removed; the prefix may include the trailing
dot. For example,
`datawave.security.elytron.jwt-enabled=true` becomes `jwt-enabled=true`.
Higher-precedence property sources win when a key occurs more than once.
Values are exported as strings after resolving Spring `${...}` placeholders
against the full remote configuration, including properties outside the prefix.
References use original, unstripped keys and respect property-source precedence.
`spring.application.name` is set to `--application-name` before placeholder
resolution, overriding any remote value for that property.
Nested references and `${name:default}` values are supported. Only exported
values and their dependencies are resolved; unrelated placeholders are ignored.
Unresolved references without defaults and circular references fail the command
before the output file is replaced.

`--output-file` is optional. When omitted, properties are written to standard
output without closing it; logs go to standard error and the Spring Boot banner
is disabled, so the output can be redirected or piped. When supplied, it must
have exactly one nonblank value specifying an absolute file path in an existing
directory. On success, the file is atomically replaced.
Invalid arguments, HTTP errors, empty results,
invalid matching properties, and write failures fail the command with a nonzero
exit status, leaving any existing output unchanged. The destination filesystem
must support atomic moves. The command does not start a web server or log
property values.
