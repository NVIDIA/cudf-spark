# FlatBuffers Classes

This directory contains the FlatBuffers schema files for messages used by
the RAPIDS Spark plugin.

## Building the FlatBuffers Compiler

Build the compiler 1.11.0 version: 
```
git clone https://github.com/google/flatbuffers.git
cd flatbuffers
git checkout 1.11.0
mkdir build
cd build
cmake ..
make
```

You can install it or otherwise use `flatbuffers/build/flatc`

## Generating the FlatBuffers Classes

From this directory:

```
flatc --java -o ../java/ --gen-mutable *.fbs
```
