package kafdrop.spi;

import org.apache.kafka.common.serialization.Deserializer;

public interface DeserializerFactory {
    Deserializer<Object> getDeserializer(String topicName);

    boolean supportsTopic(String topicName);
}
