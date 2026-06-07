# acl-shim
Re-provides `java.security.acl.Group` (removed in JDK 14) so the EOL PicketBox login-module API compiles/runs under JDK 17, via `--patch-module java.base`.
Interim bridge — the proper fix is migrating web-services/security off PicketBox to WildFly Elytron.
