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
import java.util.Random;

@Immutable
@XmlJavaTypeAdapter(value = LeadershipGroup.XmlAdapter.class)
@JsonSerialize(using = LeadershipGroup.JsonSerializeAdapter.class)
@JsonDeserialize(using = LeadershipGroup.JsonDeserializeAdapater.class)
public final class LeadershipGroup extends StringValue {
    protected LeadershipGroup(final String value) {
        super(value);
    }

    public static LeadershipGroup valueOf(String value) {
        return new LeadershipGroup(StringUtils.trimToEmpty(value));
    }

    public static LeadershipGroup valueOf(StringValue value) {
        return LeadershipGroup.valueOf(value.get());
    }

    public static LeadershipGroup random() {
        return LeadershipGroup.valueOf(Integer.valueOf(new Random().nextInt()).toString());
    }

    public static class XmlAdapter extends JaxbStringValueAdapter<LeadershipGroup> {
        @Override
        protected LeadershipGroup createNewInstance(String value) {
            return LeadershipGroup.valueOf(value);
        }
    }

    public static class JsonDeserializeAdapater extends JsonDeserializer<LeadershipGroup> {

        @Override public LeadershipGroup deserialize(
                final JsonParser jp, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
            return LeadershipGroup.valueOf(jp.getValueAsString());
        }
    }

    public static class JsonSerializeAdapter extends JsonSerializer<LeadershipGroup> {
        @Override public void serialize(
                final LeadershipGroup value, final JsonGenerator jgen, final SerializerProvider provider) throws
                                                                                                           IOException,
                                                                                                           JsonProcessingException {
            jgen.writeString(value.get());
        }
    }
}
