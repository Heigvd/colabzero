package ch.colabproject.colabzero.api.rest.utils;

import javax.json.bind.Jsonb;
import javax.json.bind.JsonbBuilder;
import javax.json.bind.JsonbConfig;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

/**
 *
 * @author Hippolyte
 */
@Provider
public class JsonbProvider implements ContextResolver<Jsonb> {

    /**
     *
     */
    //ObjectMapper mapper;
    /**
     * {@inheritDoc}
     */
    @Override
    public Jsonb getContext(Class<?> aClass) {
        return JsonbProvider.getMapper();
    }

    /**
     *
     * @return an ObjectMapper
     */
    public static Jsonb getMapper() {
        CharSeqDeserializer deserializer = new CharSeqDeserializer();
        JsonbConfig config = new JsonbConfig().withFormatting(true).withDeserializers(deserializer);

        return JsonbBuilder.create(config);
    }
}
