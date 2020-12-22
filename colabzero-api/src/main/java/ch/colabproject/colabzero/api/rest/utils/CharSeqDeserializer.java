
package ch.colabproject.colabzero.api.rest.utils;

import java.lang.reflect.Type;
import javax.json.bind.serializer.DeserializationContext;
import javax.json.bind.serializer.JsonbDeserializer;
import javax.json.stream.JsonParser;

/**
 *
 */
public class CharSeqDeserializer implements JsonbDeserializer<CharSequence> {

    public CharSeqDeserializer() {
    }

    @Override
    public CharSequence deserialize(JsonParser parser, DeserializationContext ctx, Type rtType) {
        return parser.getString();
    }
}
