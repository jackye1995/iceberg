/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.expressions;

import org.apache.iceberg.AssertHelpers;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.Assert;
import org.junit.Test;

public class TestLiteralParser {

  @Test
  public void testSerializeAboveMax() throws Exception {
    String expected = "{\"type\":\"above-max\"}";
    String actual = LiteralParser.toJson(Literals.AboveMax.INSTANCE);
    Assert.assertEquals("Serialized result should match", expected, actual);
  }

  @Test
  public void testDeserializeAboveMax() throws Exception {
    Literal<?> expected = Literals.AboveMax.INSTANCE;
    Literal<?> actual = LiteralParser.fromJson("{\"type\":\"above-max\"}");
    Assert.assertEquals("Deserialized result should match", expected, actual);
  }

  @Test
  public void testSerializeBelowMin() throws Exception {
    String expected = "{\"type\":\"below-min\"}";
    String actual = LiteralParser.toJson(Literals.BelowMin.INSTANCE);
    Assert.assertEquals("Serialized result should match", expected, actual);
  }

  @Test
  public void testDeserializeBelowMin() throws Exception {
    Literal<?> expected = Literals.BelowMin.INSTANCE;
    Literal<?> actual = LiteralParser.fromJson("{\"type\":\"below-min\"}");
    Assert.assertEquals("Deserialized result should match", expected, actual);
  }

  @Test
  public void testSerializeBoolean() throws Exception {
    String expected = "{\"type\":\"boolean\",\"value\":\"\\u0001\"}";
    String actual = LiteralParser.toJson(Literals.from(true));
    Assert.assertEquals("Serialized result should match", expected, actual);
  }

  @Test
  public void testDeserializeBoolean() throws Exception {
    Literal<?> expected = Literals.from(true);
    Literal<?> actual = LiteralParser.fromJson("{\"type\":\"boolean\",\"value\":\"\\u0001\"}");
    Assert.assertEquals("Deserialized result should match", expected, actual);
  }

  @Test
  public void testDeserializeBadValue() throws Exception {
    AssertHelpers.assertThrows("Should fail to deserialize",
        IllegalArgumentException.class,
        "Cannot parse type string to primitive: integer",
        () -> LiteralParser.fromJson("{\"type\":\"integer\",\"value\":\"test\"}"));
  }

  @Test
  public void testDeserializeBaseLiteralNoValue() throws Exception {
    AssertHelpers.assertThrows("Should fail to deserialize",
        IllegalArgumentException.class,
        "asdfasdfasdf",
        () -> LiteralParser.fromJson("{\"type\":\"integer\"}"));
  }
}
