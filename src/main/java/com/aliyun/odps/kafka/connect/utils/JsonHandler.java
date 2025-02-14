package com.aliyun.odps.kafka.connect.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.json.JsonConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonHandler {

  static final ObjectMapper objectMapper = new ObjectMapper();

  public static JsonNode extractPayLoad(Schema schema, Object message, boolean schemaEnable) {
    Map<String, Boolean> props = Collections.singletonMap("schemas.enable", schemaEnable);
    final JsonConverter converter = new JsonConverter();
    converter.configure(props, false);
    JsonNode payload;
    try {
      payload = objectMapper.readTree(converter.fromConnectData("topic", schema, message));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return payload;
  }

  public static String getJsonString(Object jsonNode) throws JsonProcessingException {
    return objectMapper.writeValueAsString(jsonNode);
  }

  public static String readJson(String jsonPath) {
    InputStream inputStream = ConfigHelper.class.getClassLoader().getResourceAsStream(jsonPath);
    assert inputStream != null;
    BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
    StringBuilder stringBuilder = new StringBuilder();
    String line;
    try {
      while ((line = reader.readLine()) != null) {
        stringBuilder.append(line);
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    return stringBuilder.toString();
  }

  public static Map<String, Object> json2Map(String json) throws JsonProcessingException {
    return objectMapper.readValue(json, HashMap.class);
  }
}
