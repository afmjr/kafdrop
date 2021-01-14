package kafdrop.util;

import com.fasterxml.jackson.databind.deser.DeserializerFactory;
import org.apache.avro.file.CodecFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.vermiculus.kafdrop.spi.ExternalDeserializerFactory;

import java.net.URL;
import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.ServiceLoader;

public class ExternalDeserializer implements MessageDeserializer {
    private static Logger logger = LoggerFactory.getLogger(ExternalDeserializer.class);
    final Deserializer externalDeserializer;
    private String topic;

    public ExternalDeserializer(Deserializer deserializer) {
        externalDeserializer = deserializer;
    }

    public static MessageDeserializer lookup(String topicName) {
        logger.trace("Lookup external deserializer for topic: " + topicName);
        ServiceLoader<ExternalDeserializerFactory> loader = ServiceLoader.load(ExternalDeserializerFactory.class);

        for (ExternalDeserializerFactory factory : loader) {
            Deserializer<Object> deserializer = factory.getDeserializer(topicName);
            if (factory.supportsTopic(topicName)) {
                logger.trace("Will use deserializer: " + deserializer.getClass().getSimpleName());
                return new ExternalDeserializer(deserializer);
            }
            else {
                logger.info("Found Deserializer ({}) doesn't support topic {}",
                        deserializer.getClass().getName(), topicName);
            }
        }
        logger.info("No external deserializer found. Will use default!");
        return new DefaultMessageDeserializer();
    }

    @Override
    public String deserializeMessage(ByteBuffer buffer) {
        return externalDeserializer.deserialize(topic, buffer.array()).toString();
    }

}
