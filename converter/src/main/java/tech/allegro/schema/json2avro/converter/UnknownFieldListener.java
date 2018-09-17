package tech.allegro.schema.json2avro.converter;

public interface UnknownFieldListener {

	void onUnknownField(String name, Object value, String path);
}
