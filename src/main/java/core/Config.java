package core;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.CopyOption;
import java.nio.file.Files;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

public class Config {

  private static final Config config = new Config();

  private Properties properties = new Properties();

  private Config() {
    loadProperties();
  }

  public static synchronized Config getInstance() {
    return config;
  }

  public void deleteConfig() {
    try {
      Files.deleteIfExists(new File("config.ini").toPath());
    }
    catch(Exception e) {
      e.printStackTrace();
    }

  }

  public void copyDefaultConfig() {
    try {
      File propertiesFile = new File("config.ini");
      Files.copy(getClass().getResourceAsStream("/config.ini"), propertiesFile.toPath());
    }
    catch(Exception e) {
      e.printStackTrace();
    }
  }

  public void loadProperties() {
    try {
      File propertiesFile = new File("config.ini");

      if(!propertiesFile.exists() || !propertiesFile.isFile()) {
        copyDefaultConfig();
        System.exit(1); // force exit/allow developer to configure ini before launching in live capacity
      }

      InputStream istream = new FileInputStream(propertiesFile);
      properties.clear();
      properties.load(istream);
      istream.close();
    }
    catch(Exception e) {
      e.printStackTrace();
    }
  }

  public String get(String key) {
    return getStringProperty(key, "");
  }

  public String get(String key, String defaultValue) {
    return getStringProperty(key, defaultValue);
  }

  public Object getProperty(String key) {
    if(properties.containsKey(key)) {
      return properties.get(key);
    }
    else {
      return "";
    }
  }

  public String getStringProperty(String key, String defaultValue) {
    return (String)properties.getProperty(key, defaultValue);
  }

  public void put(String key, String value) {
    properties.put(key, value);
  }

  public Set<Entry<Object, Object>> entrySet() {
    return properties.entrySet();
  }

}
