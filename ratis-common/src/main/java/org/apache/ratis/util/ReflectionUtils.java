/*
 * *
 *  * Licensed to the Apache Software Foundation (ASF) under one
 *  * or more contributor license agreements.  See the NOTICE file
 *  * distributed with this work for additional information
 *  * regarding copyright ownership.  The ASF licenses this file
 *  * to you under the Apache License, Version 2.0 (the
 *  * "License"); you may not use this file except in compliance
 *  * with the License.  You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package org.apache.ratis.util;


import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.WeakHashMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Reflection related utility methods.
 */
public interface ReflectionUtils {
  final class Constructors {
    private Constructors() {}

    /**
     * Cache of constructors for each class. Pins the classes so they
     * can't be garbage collected until ReflectionUtils can be collected.
     */
    private static final Map<List<Class<?>>, Constructor<?>> CONSTRUCTORS
        = new ConcurrentHashMap<>();

    private static <T> Constructor<T> get(Class<T> clazz, Class<?>[] argClasses)
        throws NoSuchMethodException {
      Objects.requireNonNull(clazz, "clazz == null");

      final List<Class<?>> key = new ArrayList<>(argClasses.length + 1);
      key.add(clazz);
      key.addAll(Arrays.asList(argClasses));

      @SuppressWarnings("unchecked")
      Constructor<T> ctor = (Constructor<T>) CONSTRUCTORS.get(key);
      if (ctor == null) {
        ctor = clazz.getDeclaredConstructor(argClasses);
        ctor.setAccessible(true);
        CONSTRUCTORS.put(key, ctor);
      }
      return ctor;
    }
  }

  final class Classes {
    private static final Class<?>[] EMPTY_ARRAY = {};

    private static final Map<ClassLoader, Map<String, WeakReference<Class<?>>>>
        CLASSES = new WeakHashMap<>();

    /** Sentinel value to store negative cache results in {@link #CLASSES}. */
    private static final Class<?> NEGATIVE_CACHE_SENTINEL = NegativeCacheSentinel.class;

    /**
     * A unique class which is used as a sentinel value in the caching
     * for getClassByName. {@link #getClassByNameOrNull(String)}
     */
    private static final class NegativeCacheSentinel {}

    private static final ClassLoader CLASS_LOADER = Optional.ofNullable(
            Thread.currentThread().getContextClassLoader()).orElseGet(ReflectionUtils.class::getClassLoader);

    private static Map<String, WeakReference<Class<?>>> getClassMap() {
      Map<String, WeakReference<Class<?>>> map;
      synchronized (CLASSES) {
        map = CLASSES.get(CLASS_LOADER);
        if (map == null) {
          map = Collections.synchronizedMap(new WeakHashMap<>());
          CLASSES.put(CLASS_LOADER, map);
        }
      }
      return map;
    }
  }

  static ClassLoader getClassLoader() {
    return Classes.CLASS_LOADER;
  }

  /**
   * Load a class by name, returning null rather than throwing an exception
   * if it couldn't be loaded. This is to avoid the overhead of creating
   * an exception.
   *
   * @param name the class name
   * @return the class object, or null if it could not be found.
   */
  static Class<?> getClassByNameOrNull(String name) {
    final Map<String, WeakReference<Class<?>>> map = Classes.getClassMap();

    Class<?> clazz = null;
    WeakReference<Class<?>> ref = map.get(name);
    if (ref != null) {
      clazz = ref.get();
    }

    if (clazz == null) {
      try {
        clazz = Class.forName(name, true, Classes.CLASS_LOADER);
      } catch (ClassNotFoundException e) {
        // Leave a marker that the class isn't found
        map.put(name, new WeakReference<>(Classes.NEGATIVE_CACHE_SENTINEL));
        return null;
      }
      // two putters can race here, but they'll put the same class
      map.put(name, new WeakReference<>(clazz));
      return clazz;
    } else if (clazz == Classes.NEGATIVE_CACHE_SENTINEL) {
      return null; // not found
    } else {
      // cache hit
      return clazz;
    }
  }

  /**
   * Load a class by name.
   *
   * @param name the class name.
   * @return the class object.
   * @throws ClassNotFoundException if the class is not found.
   */
  static Class<?> getClassByName(String name) throws ClassNotFoundException {
    Class<?> ret = getClassByNameOrNull(name);
    if (ret == null) {
      throw new ClassNotFoundException("Class " + name + " not found");
    }
    return ret;
  }

  /**
   * Create an object for the given class using its default constructor.
   */
  static <T> T newInstance(Class<T> clazz) {
    return newInstance(clazz, Classes.EMPTY_ARRAY);
  }

  /**
   * Create an object for the given class using the specified constructor.
   *
   * @param clazz class of which an object is created
   * @param argClasses argument classes of the constructor
   * @param args actual arguments to be passed to the constructor
   * @param <T> class type of clazz
   * @return a new object
   */
  static <T> T newInstance(Class<T> clazz, Class<?>[] argClasses, Object... args) {
    final Constructor<T> ctor;
    try {
      ctor = Constructors.get(clazz, argClasses);
    } catch (NoSuchMethodException e) {
      throw new UnsupportedOperationException(
          "Unable to find suitable constructor for class " + clazz.getName()
          + ", argument classes = " + Arrays.toString(argClasses), e);
    }
    return instantiate(clazz.getName(), ctor, args);
  }

  static <T> T instantiate(final String className, Constructor<T> ctor, Object[] ctorArgs) {
    try {
      ctor.setAccessible(true);
      return ctor.newInstance(ctorArgs);
    } catch (IllegalAccessException e) {
      throw new UnsupportedOperationException(
          "Unable to access specified class " + className, e);
    } catch (InstantiationException e) {
      throw new UnsupportedOperationException(
          "Unable to instantiate specified class " + className, e);
    } catch (InvocationTargetException e) {
      throw new UnsupportedOperationException(
          "Constructor threw an exception for " + className, e);
    }
  }

  /** Is the given object an instance of one of the given classes? */
  static boolean isInstance(Object obj, Class<?>... classes) {
    for(Class<?> c : classes) {
      if (c.isInstance(obj)) {
        return true;
      }
    }
    return false;
  }

  static <BASE> Class<? extends BASE> getClass(
      String subClassName, Class<BASE> base) {
    try {
      return getClassByName(subClassName).asSubclass(base);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException("Failed to get class "
          + subClassName + " as a subclass of " + base, e);
    }
  }

  static Exception instantiateException(Class<? extends Exception> cls,
      String message, Exception from) throws Exception {
    Constructor<? extends Exception> cn = cls.getConstructor(String.class);
    cn.setAccessible(true);
    Exception ex = cn.newInstance(message);
    if (from != null) {
      ex.initCause(from);
    }
    return ex;
  }
}
