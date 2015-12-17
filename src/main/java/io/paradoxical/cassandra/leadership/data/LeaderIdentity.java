package io.paradoxical.cassandra.leadership.data;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import io.paradoxical.common.valuetypes.StringValue;
import io.paradoxical.common.valuetypes.adapters.xml.JaxbStringValueAdapter;
import jdk.nashorn.internal.ir.annotations.Immutable;
import org.apache.commons.lang3.StringUtils;

import javax.xml.bind.annotation.adapters.XmlJavaTypeAdapter;
import java.io.IOException;

@Immutable
@XmlJavaTypeAdapter(value = LeaderIdentity.XmlAdapter.class)
@JsonSerialize(using = LeaderIdentity.JsonSerializeAdapter.class)
@JsonDeserialize(using = LeaderIdentity.JsonDeserializeAdapater.class)
public final class LeaderIdentity extends StringValue {
    protected LeaderIdentity(final String value) {
        super(value);
    }

    public static LeaderIdentity valueOf(String value) {
        return new LeaderIdentity(StringUtils.trimToEmpty(value));
    }

    public static LeaderIdentity valueOf(StringValue value) {
        return LeaderIdentity.valueOf(value.get());
    }

    public static class XmlAdapter extends JaxbStringValueAdapter<LeaderIdentity> {
        @Override
        protected LeaderIdentity createNewInstance(String value) {
            return LeaderIdentity.valueOf(value);
        }
    }

    public static class JsonDeserializeAdapater extends JsonDeserializer<LeaderIdentity> {

        @Override public LeaderIdentity deserialize(
                final JsonParser jp, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
            return LeaderIdentity.valueOf(jp.getValueAsString());
        }
    }

    public static class JsonSerializeAdapter extends JsonSerializer<LeaderIdentity> {
        @Override public void serialize(
                final LeaderIdentity value, final JsonGenerator jgen, final SerializerProvider provider) throws
                                                                                                         IOException,
                                                                                                         JsonProcessingException {
            jgen.writeString(value.get());
        }
    }
}
