/*
 * Copyright (c) 2026, NVIDIA CORPORATION.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.nvidia.spark.rapids.reader;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Comparator;

import scala.collection.immutable.List$;
import scala.runtime.AbstractFunction0;
import scala.runtime.BoxedUnit;

import org.junit.jupiter.api.Test;
import org.scalactic.source.Position;
import org.scalatest.Tag;
import org.scalatest.funsuite.AnyFunSuite;

/**
 * Makes the reader's plain Java JUnit tests discoverable by the existing ScalaTest runners.
 * Only public, no-argument @Test methods are supported; fixtures are local to each method.
 */
public abstract class JavaReaderSuite extends AnyFunSuite {
  protected JavaReaderSuite(Object... targets) {
    for (Object target : targets) {
      Method[] methods = target.getClass().getDeclaredMethods();
      Arrays.sort(methods, Comparator.comparing(Method::getName));
      for (Method method : methods) {
        if (method.isAnnotationPresent(Test.class)) {
          String file = target.getClass().getSimpleName() + ".java";
          registerTest(target.getClass().getSimpleName() + "." + method.getName(),
              List$.MODULE$.<Tag>empty(), new AbstractFunction0<Object>() {
                @Override
                public Object apply() {
                  try {
                    method.invoke(target);
                    return BoxedUnit.UNIT;
                  } catch (InvocationTargetException error) {
                    Throwable cause = error.getCause();
                    if (cause instanceof Error) {
                      throw (Error) cause;
                    }
                    if (cause instanceof RuntimeException) {
                      throw (RuntimeException) cause;
                    }
                    throw new AssertionError("Java reader test failed", cause);
                  } catch (ReflectiveOperationException error) {
                    throw new AssertionError("Cannot invoke Java reader test", error);
                  }
                }
              }, new Position(file, file, 1));
        }
      }
    }
  }
}
