package tech.allegro.schema.json2avro.converter;

import java.util.Deque;

public interface UnknownPropertyListener {

	void onUnknownProperty(String propertyName, Object propertyValue, String path);
}
