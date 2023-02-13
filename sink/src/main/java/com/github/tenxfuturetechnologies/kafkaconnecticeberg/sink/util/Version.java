package com.github.tenxfuturetechnologies.kafkaconnecticeberg.sink.util;

import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Version {
  private static final Logger log = LoggerFactory.getLogger(Version.class);
  private static final String PROPERTIES_PATH = "/kafka-connect-iceberg-sink.properties";
  private static final String UNKNOWN_VERSION = "unknown";
  private static final String VERSION = loadVersion();

  private Version() {}

  private static String loadVersion() {
    try {
      var props = new Properties();
      props.load(Version.class.getResourceAsStream(PROPERTIES_PATH));
      return props.getProperty("version", UNKNOWN_VERSION).trim();
    } catch (Exception e) {
      log.warn("Unable to load version from " + PROPERTIES_PATH, e);
    }
    return UNKNOWN_VERSION;
  }

  public static String getVersion() {
    return VERSION;
  }
}
