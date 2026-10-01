#!/usr/bin/env bash
# Stop on errors, unset variables, and failed commands inside pipelines.
set -euo pipefail

# Locate both checkouts; allow the Sonicweb root and member port to be overridden by the caller.
DW_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
API_ROOT="$DW_ROOT/api"
SERVICE_ROOT="$DW_ROOT/service"
SONICWEB_ROOT="${SONICWEB_ROOT:-/app/sonicweb}"
SONICWEB_SERVICE="$SONICWEB_ROOT/service"
PORT="${ANNOTATION_CACHE_PAIRING_PORT:-57991}"

# Keep generated classpaths and member logs out of the repository.
TEMP_DIR="$(mktemp -d)"
MEMBER_PID=""

# Stop the background member and remove temporary files, even when a build or probe fails.
cleanup() {
    if [[ -n "$MEMBER_PID" ]] && kill -0 "$MEMBER_PID" 2>/dev/null; then
        # Ask the member to shut down, wait briefly, then force-stop it if needed.
        kill "$MEMBER_PID" 2>/dev/null || true
        for _ in $(seq 1 5); do
            if ! kill -0 "$MEMBER_PID" 2>/dev/null; then
                break
            fi
            sleep 1
        done
        kill -KILL "$MEMBER_PID" 2>/dev/null || true
        wait "$MEMBER_PID" 2>/dev/null || true
    fi
    rm -rf "$TEMP_DIR"
}
trap cleanup EXIT

# Install the annotation-cache API and service so the member uses their built classes and resolved dependencies.
mvn -Dmaven.build.cache.enabled=false -f "$DW_ROOT/pom.xml" -pl service -am clean install
# Compile Sonicweb's service and dependencies so the client probe class is available.
mvn -Dmaven.build.cache.enabled=false -f "$SONICWEB_ROOT/pom.xml" -pl service -am test-compile
# Write each service's resolved runtime dependencies to a separate classpath file.
mvn -Dmaven.build.cache.enabled=false -f "$SERVICE_ROOT/pom.xml" -DincludeScope=runtime -Dmdep.outputFile="$TEMP_DIR/service-classpath" dependency:build-classpath
mvn -Dmaven.build.cache.enabled=false -f "$SONICWEB_SERVICE/pom.xml" -DincludeScope=runtime -Dmdep.outputFile="$TEMP_DIR/sonicweb-classpath" dependency:build-classpath

# Use each application's own classes and runtime dependencies to test their real member/client pairing.
# The member probe alone comes from API test-classes; the member API artifact and dependencies come from the service build.
MEMBER_CP="$SERVICE_ROOT/target/classes:$API_ROOT/target/test-classes:$(<"$TEMP_DIR/service-classpath")"
SONICWEB_CP="$SONICWEB_SERVICE/target/classes:$SONICWEB_SERVICE/target/test-classes:$(<"$TEMP_DIR/sonicweb-classpath")"
# Start the member in the background and capture its output so readiness and startup failures can be checked.
java -cp "$MEMBER_CP" datawave.microservice.annotationCache.api.AnnotationCacheHazelcastMemberProbe "$PORT" >"$TEMP_DIR/member.log" 2>&1 &
MEMBER_PID=$!

# Wait up to one minute for the member's READY message; report its log if it exits or times out.
for _ in $(seq 1 60); do
    if grep -q "READY $PORT" "$TEMP_DIR/member.log"; then
        break
    fi
    if ! kill -0 "$MEMBER_PID" 2>/dev/null; then
        cat "$TEMP_DIR/member.log" >&2
        echo "Hazelcast member exited before becoming ready" >&2
        exit 1
    fi
    sleep 1
done
if ! grep -q "READY $PORT" "$TEMP_DIR/member.log"; then
    cat "$TEMP_DIR/member.log" >&2
    echo "Timed out waiting for Hazelcast member" >&2
    exit 1
fi

# Print the member version, then run the Sonicweb probe with a 45-second limit.
grep '^READY ' "$TEMP_DIR/member.log"
timeout --signal=TERM --kill-after=5s 45s java -cp "$SONICWEB_CP" datawave.microservice.sonicweb.service.annotation.repository.AnnotationCacheHazelcastClientProbe "$PORT"
