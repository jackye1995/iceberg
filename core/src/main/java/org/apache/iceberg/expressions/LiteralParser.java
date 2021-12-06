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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import org.apache.iceberg.exceptions.RuntimeIOException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.JsonUtil;

public class LiteralParser {

  private static final String TYPE = "type";
  private static final String VALUE = "value";
  private static final String ABOVE_MAX = "above-max";
  private static final String BELOW_MIN = "below-min";

  public static String toJson(Literal<?> literal) {
    try (StringWriter writer = new StringWriter()) {
      JsonGenerator generator = JsonUtil.factory().createGenerator(writer);
      toJson(literal, generator);
      generator.flush();
      return writer.toString();
    } catch (IOException e) {
      throw new RuntimeIOException(e, "Failed to write json for: %s", literal);
    }
  }

  public static void toJson(Literal<?> literal, JsonGenerator generator) throws IOException {
    generator.writeStartObject();

    if (literal instanceof Literals.AboveMax) {
      generator.writeStringField(TYPE, ABOVE_MAX);
    } else if (literal instanceof Literals.BelowMin) {
      generator.writeStringField(TYPE, BELOW_MIN);
    } else {
      ValidationException.check(literal instanceof Literals.BaseLiteral,
          "Unexpected literal type for serialization: %s", literal.getClass().getName());
      Literals.BaseLiteral<?> baseLiteral = (Literals.BaseLiteral<?>) literal;
      generator.writeStringField(TYPE, baseLiteral.typeId().name().toLowerCase(Locale.ENGLISH));
      String serializedValue = StandardCharsets.UTF_8.decode(literal.toByteBuffer()).toString();
      generator.writeStringField(VALUE, serializedValue);
    }

    generator.writeEndObject();
  }

  public static Literal<?> fromJson(String json) throws IOException {
    return fromJson(JsonUtil.mapper().readValue(json, JsonNode.class));
  }

  public static Literal<?> fromJson(JsonNode node) throws IOException {
    String typeStr = JsonUtil.getString(TYPE, node);
    if (typeStr.equals(ABOVE_MAX)) {
      return Literals.AboveMax.INSTANCE;
    } else if (typeStr.equals(BELOW_MIN)) {
      return Literals.BelowMin.INSTANCE;
    } else {
      Type type = Types.fromPrimitiveString(typeStr);
      String serializedValue = JsonUtil.getString(VALUE, node);
      Object value = Conversions.fromByteBuffer(
          type, ByteBuffer.wrap(serializedValue.getBytes(StandardCharsets.UTF_8)));
      return Literals.from(value);
    }
  }

}