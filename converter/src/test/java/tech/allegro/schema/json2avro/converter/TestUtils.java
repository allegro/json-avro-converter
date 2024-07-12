package tech.allegro.schema.json2avro.converter;

import com.google.common.io.Resources;

import java.io.IOException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TestUtils {
    @SuppressWarnings("UnstableApiUsage")
    public static String readResource(final String name) throws IOException {
        final URL resource = Resources.getResource(name);
        return Resources.toString(resource, StandardCharsets.UTF_8);
    }

    public static <T> List<T> toList(final Iterator<T> iterator) {
        final List<T> list = new ArrayList<>();
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return list;
    }
}
