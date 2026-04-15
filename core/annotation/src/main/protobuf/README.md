The `SegmentV1.proto` definition uses

* `protoc` installed from [protoc-3.16.3-linux-x86_64.zip](https://github.com/protocolbuffers/protobuf/releases/tag/v3.16.3) 
* `googleapis` cloned via `git clone https://fuchsia.googlesource.com/third_party/googleapis`
* `protoc-gen-jsonschema` cloned via `git clone git@github.com:pubg/protoc-gen-jsonschema.git`

## Installing `protoc`

```bash
curl -JLO https://github.com/protocolbuffers/protobuf/releases/download/v3.16.3/protoc-3.16.3-linux-x86_64.zip
mkdir protoc && cd protoc
unzip ../protoc-3.16.3-linux-x86_64.zip
sudo cp -a bin/protoc /usr/local/bin
sudo cp -a include/google /usr/local/include
```

## Installing `googleapis`

```bash
git clone https://fuchsia.googlesource.com/third_party/googleapis
cd googleapis
sudo cp -a google/* /usr/local/include/google
```

## Installing `protoc-gen-jsonschema`

Ensure that `go` 1.24.3 or later is installed already.

```bash
git clone git@github.com:pubg/protoc-gen-jsonschema.git
cd protoc-gen-jsonschema
go install github.com/pubg/protoc-gen-jsonschema
```

## Optional: build via Maven

When you need to regenerate `AnnotationV1.proto` Java sources, run the protobuf plugin that is configured in `core/annotation/pom.xml`. It compiles the `.proto` definitions with the same protobuf toolchain referenced in the Maven build and writes the generated Java directly into `core/annotation/src/main/java/datawave/annotation/protobuf/v1`:

```bash
mvn -pl core/annotation protobuf:compile
```

Generated files stay alongside the checked-in sources and no additional source-path configuration is required.
