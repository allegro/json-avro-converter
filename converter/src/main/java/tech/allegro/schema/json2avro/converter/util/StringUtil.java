package tech.allegro.schema.json2avro.converter.util;

import java.nio.charset.StandardCharsets;
import java.util.Base64;

public class StringUtil {

  public static boolean isBase64(String value) {
    return value != null && value.matches("^([A-Za-z0-9+/]{4})*([A-Za-z0-9+/]{3}=|[A-Za-z0-9+/]{2}==)?$");
  }

  public static String decodeBase64(String string) {
    if (isBase64(string)) {
      byte[] decoded = Base64.getDecoder().decode(string);
      return new String(decoded, StandardCharsets.UTF_8);
    }
    return string;
  }
}
