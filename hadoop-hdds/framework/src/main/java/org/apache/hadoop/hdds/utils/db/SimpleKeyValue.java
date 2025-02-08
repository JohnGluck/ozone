/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hadoop.hdds.utils.db;

import java.util.Objects;

/**
 * Represents a class for a simple key-value pair with information about the size.
 *
 * @param <KEY> Type of key value.
 * @param <VALUE> Type of value.
 */
public class SimpleKeyValue<KEY, VALUE> implements Table.KeyValue<KEY, VALUE> {
  private final KEY key;

  private final VALUE value;

  private final int rawSize;

  SimpleKeyValue(KEY key, VALUE value) {
    this(key, value, 0);
  }

  SimpleKeyValue(KEY key, VALUE value, int rawSize) {
    this.key = key;
    this.value = value;
    this.rawSize = rawSize;
  }

  @Override
  public KEY getKey() {
    return key;
  }

  @Override
  public VALUE getValue() {
    return value;
  }

  @Override
  public int getRawSize() {
    return rawSize;
  }

  @Override
  public String toString() {
    return "(key=" + key + ", value=" + value + ")";
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }

    if (!(other instanceof SimpleKeyValue)) {
      return false;
    }

    SimpleKeyValue<?, ?> otherKv = (SimpleKeyValue<?, ?>) other;

    return getKey().equals(otherKv.getKey())
               && getValue().equals(otherKv.getValue())
               && getRawSize() == otherKv.getRawSize();
  }

  @Override
  public int hashCode() {
    return Objects.hash(getKey(), getValue(), getRawSize());
  }
}
