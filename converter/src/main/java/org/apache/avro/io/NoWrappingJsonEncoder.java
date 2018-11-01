package org.apache.avro.io;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import org.apache.avro.Schema;
import org.apache.avro.io.parsing.Symbol;
import org.codehaus.jackson.JsonGenerator;

public class NoWrappingJsonEncoder extends JsonEncoder {
    public NoWrappingJsonEncoder(Schema sc, OutputStream out) throws IOException {
        super(sc, out);
    }

    public NoWrappingJsonEncoder(Schema sc, OutputStream out, boolean pretty) throws IOException {
        super(sc, out, pretty);
    }

    public NoWrappingJsonEncoder(Schema sc, JsonGenerator out) throws IOException {
        super(sc, out);
    }

    @Override
    public void writeBytes(byte[] bytes, int start, int len) throws IOException {
        byte[] slice = new byte[len];
        System.arraycopy(bytes, start, slice, 0, len);

        byte[] encoded = Base64.getEncoder().encodeToString(slice).getBytes(StandardCharsets.UTF_8);
        super.writeBytes(encoded, 0, encoded.length);
    }

    @Override
    public void writeIndex(int unionIndex) throws IOException {
        parser.advance(Symbol.UNION);
        Symbol.Alternative top = (Symbol.Alternative) parser.popSymbol();
        Symbol symbol = top.getSymbol(unionIndex);
        parser.pushSymbol(symbol);
    }
}
