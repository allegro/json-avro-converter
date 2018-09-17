package tech.allegro.schema.json2avro.converter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogOnUnknownProperty implements UnknownPropertyListener {
	
	private final Logger logger;
	
	public LogOnUnknownProperty() {
		this(null);
	}

	public LogOnUnknownProperty(Logger logger) {
		if (logger == null)
			logger = LoggerFactory.getLogger(LogOnUnknownProperty.class);
		this.logger = logger;
	}



	@Override
	public void onUnknownProperty(String propertyName, Object propertyValue, String path) {
		logger.warn("Unknow field {} with value {}", path, propertyName);
	}

}
