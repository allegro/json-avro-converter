package tech.allegro.schema.json2avro.converter;

import java.util.Deque;

import static java.util.stream.Collectors.joining;

public class PathsPrinter {

    public static String print(Deque<String> path) {
        return path.stream().collect(joining("."));
    }

    public static String print(Deque<String> path, String additionalSegment) {
    	if (path.isEmpty()) {
    		return additionalSegment;
    	}
        return print(path) + "." + additionalSegment;
    }

}
