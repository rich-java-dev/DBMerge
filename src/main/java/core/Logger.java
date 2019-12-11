package core;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class Logger {

  private static final Logger logger = new Logger();

  private Map<String, BufferedWriter> logMap = new HashMap<>();

  private Logger() {
  }

  public static synchronized Logger getInstance() {
    return logger;
  }

  public BufferedWriter get(String key) throws IOException {
    if(logMap.containsKey(key)) {
      return logMap.get(key);
    }
    else {
      BufferedWriter writer = new BufferedWriter(new FileWriter(key));
      logMap.put(key, writer);
      return writer;
    }
  }

  public void log(String logger, String msg, Object... params) {
    try {
      BufferedWriter writer = get(logger);

      if(params != null && params.length > 0) {
        msg = String.format(msg, params);
      }

      System.out.println(msg);
      StringBuffer timeBuffer = new StringBuffer(new Timestamp(System.currentTimeMillis()).toString());
      while(timeBuffer.length() < 23) {
        timeBuffer.append('0');
      }

      writer.write(String.format("[%s] %s", timeBuffer.toString(), msg) + "\r\n");

    }
    catch(Exception e) {
      e.printStackTrace();
    }
  }

  public void close() throws IOException {
    for(BufferedWriter writer : logMap.values()) {
      writer.close();
    }
  }

}
