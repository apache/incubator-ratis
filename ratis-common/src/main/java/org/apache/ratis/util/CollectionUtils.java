/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.util;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public interface CollectionUtils {
  static <T> T min(T left, T right, Comparator<T> comparator) {
    return comparator.compare(left, right) < 0? left: right;
  }

  static <T extends Comparable<T>> T min(T left, T right) {
    return min(left, right, Comparator.naturalOrder());
  }

  /**
   *  @return the next element in the iteration right after the given element;
   *          if the given element is not in the iteration, return the first one
   */
  static <T> T next(final T given, final Iterable<T> iteration) {
    Objects.requireNonNull(given, "given == null");
    final Iterator<T> i = Objects.requireNonNull(iteration, "iteration == null").iterator();
    Preconditions.assertTrue(i.hasNext(), "iteration is empty.");

    final T first = i.next();
    for(T current = first; i.hasNext(); ) {
      final T next = i.next();
      if (given.equals(current)) {
        return next;
      }
      current = next;
    }
    return first;
  }

  /**
   *  @return a randomly picked element which is not the given element.
   */
  static <T> T random(final T given, Iterable<T> iteration) {
    Objects.requireNonNull(given, "given == null");
    Objects.requireNonNull(iteration, "iteration == null");

    final List<T> list = StreamSupport.stream(iteration.spliterator(), false)
        .filter(e -> !given.equals(e))
        .collect(Collectors.toList());
    final int size = list.size();
    return size == 0? null: list.get(ThreadLocalRandom.current().nextInt(size));
  }

  static <INPUT, OUTPUT> Iterable<OUTPUT> as(
      Iterable<INPUT> iteration, Function<INPUT, OUTPUT> converter) {
    return () -> new Iterator<OUTPUT>() {
      private final Iterator<INPUT> i = iteration.iterator();
      @Override
      public boolean hasNext() {
        return i.hasNext();
      }

      @Override
      public OUTPUT next() {
        return converter.apply(i.next());
      }
    };
  }

  static <INPUT, OUTPUT> Iterable<OUTPUT> as(
      INPUT[] array, Function<INPUT, OUTPUT> converter) {
    return as(Arrays.asList(array), converter);
  }

  static <K, V> V putNew(K key, V value, Map<K, V> map, Supplier<Object> name) {
    final V returned = map.put(key, value);
    Preconditions.assertTrue(returned == null,
        () -> "Entry already exists for key " + key + " in map " + name.get());
    return value;
  }

  static <K, V> void replaceExisting(K key, V oldValue, V newValue, Map<K, V> map, Supplier<Object> name) {
    final boolean replaced = map.replace(key, oldValue, newValue);
    Preconditions.assertTrue(replaced,
        () -> "Entry not found for key " + key + " in map " + name.get());
  }

  static <K, V> void removeExisting(K key, V value, Map<K, V> map, Supplier<Object> name) {
    final boolean removed = map.remove(key, value);
    Preconditions.assertTrue(removed,
        () -> "Entry not found for key " + key + " in map " + name.get());
  }

  static <K, V> V computeIfAbsent(ConcurrentMap<K, V> map, K key, Supplier<V> supplier,
      Runnable actionIfAbsent) {
    V v = map.get(key);
    if (v != null) {
      return v;
    }
    V newValue = supplier.get();
    v = map.putIfAbsent(key, newValue);
    if (v != null) {
      return v;
    }
    actionIfAbsent.run();
    return newValue;
  }

  static <K, V> V computeIfAbsent(ConcurrentMap<K, V> map, K key, Supplier<V> supplier) {
    return computeIfAbsent(map, key, supplier, () -> {
    });
  }
}
