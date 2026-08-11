# Host Memory Leak Detection

Both Java and CUDA tend to cause issues with tools that try to detect host memory leaks. This project uses
both these technologies. We have tried many tools like valgrind and memleax but with limited success. So
we finally decided to make our own tool, which is a bit of a hack.

## Limitations

This tool is very specific to libcudf in that it only overrides C++ `new` and `delete` operations. It does
not cover `malloc`. Java uses `malloc` internally, but not `new` or `delete`. This helps to reduce the
number of allocations that need to be tracked.

The tool also only prints out the allocation log to `stderr`. This is not on purpose, it was just to
get something working quickly. Which is the same reason it uses a test log and why there are extra
"\n"s inserted into the output to make sure that it didn't matter what was already written out on
stderr, that the tracking lines would each be on their own line.

In some parts of the plugin we are leaking memory on purpose because of how Spark works. We have planned to
fix this, but for the time being it is best to set `-Dai.rapids.refcount.debug=true` on the java command
line when running to make sure that the java garbage collector ran at the end and cleaned up everything
that might have leaked on purpose so they do not show up as leaks here.

The stack traces do not include anything related to java. They typically stop once you hit the JNI API.
There are often times when I was debugging a leak when I added in java stack trace code to print
things out every time a specific API was called so I could line it up with the leak and see what the
java code was doing.

## Building

I didn't even create a `Makefile` for this because 1. this is kind of hacky and you should think about
using this ahead of time, and 2, it is only 1 file anyways...

```
g++ -fPIC -c leak_detect.cpp
g++ -shared -Wl,-init,init_leak_detect -o libleak_detect.so leak_detect.o
```

## Running

Running with this change is quite a bit more complicated. Generally you set `LD_PRELOAD` to point to the full
path to where `libleak_detect.so` is located and capture stderr for later processing

```
LD_PRELOAD=$PWD/libleak_detect.so java -cp ... com.nvidia.MyTest 2>alloc_log.txt
```

But things start to get complicated if you are running pyspark or the pytest integration tests. This is because
python uses `new` and `delete`, appears to purposely leak some memory on shutdown, and because we just write
the log to stderr, so there is no locking to keep different processes from messing up the log.

### Spark modifications for python testing

To work around this you can apply the following patch to strip out `LD_PRELOAD` when launching python processes
from the JVM. Please note that if you are doing this with the integration tests you will need to run with
`TEST_PARALLEL=0` so that the JVM process is the first thing launched and not the python process.

```diff
diff --git a/core/src/main/scala/org/apache/spark/api/python/PythonWorkerFactory.scala b/core/src/main/scala/org/apache/spark/api/python/PythonWorkerFactory.scala
index 7b2c36bb10..cef095d716 100644
--- a/core/src/main/scala/org/apache/spark/api/python/PythonWorkerFactory.scala
+++ b/core/src/main/scala/org/apache/spark/api/python/PythonWorkerFactory.scala
@@ -158,6 +158,7 @@ private[spark] class PythonWorkerFactory(pythonExec: String, envVars: Map[String
       val pb = new ProcessBuilder(Arrays.asList(pythonExec, "-m", workerModule))
       val workerEnv = pb.environment()
       workerEnv.putAll(envVars.asJava)
+      workerEnv.remove("LD_PRELOAD")
       workerEnv.put("PYTHONPATH", pythonPath)
       // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
       workerEnv.put("PYTHONUNBUFFERED", "YES")
@@ -208,6 +209,7 @@ private[spark] class PythonWorkerFactory(pythonExec: String, envVars: Map[String
         val pb = new ProcessBuilder(command)
         val workerEnv = pb.environment()
         workerEnv.putAll(envVars.asJava)
+        workerEnv.remove("LD_PRELOAD")
         workerEnv.put("PYTHONPATH", pythonPath)
         workerEnv.put("PYTHON_WORKER_FACTORY_SECRET", authHelper.secret)
         // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
diff --git a/core/src/main/scala/org/apache/spark/deploy/PythonRunner.scala b/core/src/main/scala/org/apache/spark/deploy/PythonRunner.scala
index c3f73ed745..82203659c3 100644
--- a/core/src/main/scala/org/apache/spark/deploy/PythonRunner.scala
+++ b/core/src/main/scala/org/apache/spark/deploy/PythonRunner.scala
@@ -74,6 +74,7 @@ object PythonRunner {
     // Launch Python process
     val builder = new ProcessBuilder((Seq(pythonExec, formattedPythonFile) ++ otherArgs).asJava)
     val env = builder.environment()
+    env.remove("LD_PRELOAD")
     env.put("PYTHONPATH", pythonPath)
     // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
     env.put("PYTHONUNBUFFERED", "YES") // value is needed to be set to a non-empty string
```

### Allocation Stack Traces

By default only the address of what was allocated is printed out when something is allocated. This is
helpful in detecting if there was a memory leak, but not in telling what leaked the memory. If you set
the environment variable `LEAK_DETECT_TRACE` to anything C++ stack traces will be output with each allocation.
Not doing the stack traces speeds up the runs a lot, which is why it is off by default.

## Interpreting The Results

The included python script `find_leak.py`. If you pass the log from the tests to this script it will parse
the log an tell you what addresses leaked. Often the same address is reused multiple times so after you
have found the address that leaked you need to look through the log again to find the last place in the log
that address is referenced so you can see what the stack trace is that allocated it.
