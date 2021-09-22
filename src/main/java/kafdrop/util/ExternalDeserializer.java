package kafdrop.util;

import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;

public class ExternalDeserializer implements MessageDeserializer {

    final Deserializer<Object> deserializer;
    private String topic;

    public ExternalDeserializer(Deserializer<Object> deserializer, String topic) {
        this.deserializer = deserializer;
        this.topic = topic;
    }

    @Override
    public String deserializeMessage(ByteBuffer buffer) {
        return deserializer.deserialize(topic, buffer.array()).toString();
    }

}
