package com.aliyun.odps.kafka.connect.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class ConfigHelper {

  public static Map<String, String> getConfigFromFile(String confFile) {
    Map<String, String> props = new HashMap<>();
    // here put your specific connector class name
    // default class name use MonitorableSinkConnector in kafka source code test
    // necessary config for odps
    InputStream inputStream = ConfigHelper.class.getClassLoader().getResourceAsStream(confFile);
    Properties fixedProperties = new Properties();
    try {
      fixedProperties.load(inputStream);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    Set<String> propsList = fixedProperties.stringPropertyNames();
    for (String item : propsList) {
      props.put(item, fixedProperties.getProperty(item));
    }
    return props;
  }

}
