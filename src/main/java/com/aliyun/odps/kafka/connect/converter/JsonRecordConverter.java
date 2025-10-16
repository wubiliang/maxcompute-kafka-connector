package com.aliyun.odps.kafka.connect.converter;

import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.SimpleJsonValue;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.sink.SinkRecord;
import org.json.JSONException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Objects;

import static com.aliyun.odps.kafka.connect.utils.JsonHandler.extractPayLoad;

public class JsonRecordConverter implements RecordConverter {

  private RecordConverterBuilder.Mode mode;

  public JsonRecordConverter(RecordConverterBuilder.Mode mode) {
    this.mode = Objects.requireNonNull(mode);
  }

  @Override
  public void convert(SinkRecord in, Record out) throws IOException {
    out.setString(TOPIC, in.topic());
    out.setBigint(PARTITION, in.kafkaPartition().longValue());
    out.setBigint(OFFSET, in.kafkaOffset());
    out.setBigint(INSERT_TIME, in.timestamp());
    switch (mode) {
      case KEY:
        if (in.key() != null) {
          out.set(KEY, convertToJson(in.key()));
        }
        break;
      case VALUE:
        if (in.value() != null) {
          out.set(VALUE, convertToJson(in.value()));
        }
        break;
      case DEFAULT:
      default:
        throw new RuntimeException("Unsupported mode for jsonConverter:" + mode);
    }
  }


  public static SimpleJsonValue convertToJson(Object jsonObject) throws JSONException, IOException {
    // convert sinkRecord object to odps JsonSimple
    if (jsonObject instanceof String) {
      return new SimpleJsonValue((String) jsonObject);
    }
    // 提取payload 字段
    if (jsonObject instanceof HashMap) {
      try {
        ObjectMapper objectMapper = new ObjectMapper();
        return new SimpleJsonValue(objectMapper.writeValueAsString(jsonObject));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    if (jsonObject instanceof Struct) {
      Struct newJsonObject = (Struct) jsonObject;
      return new SimpleJsonValue(extractPayLoad(newJsonObject.schema(), newJsonObject, false));
    }
    throw new RuntimeException("no recognize type of jsonObject");
  }
}
