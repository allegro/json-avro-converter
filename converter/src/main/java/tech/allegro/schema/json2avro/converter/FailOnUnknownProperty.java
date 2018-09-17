package tech.allegro.schema.json2avro.converter;

import org.apache.avro.AvroTypeException;

public class FailOnUnknownProperty implements UnknownPropertyListener {

	@Override
	public void onUnknownProperty(String propertyName, Object propertyValue, String path) {
		throw new AvroTypeException("Field " + path + " is unknown");
	}

}
