package kafdrop.util;

import kafdrop.spi.DeserializerFactory;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        logger.info("Lookup external deserializer for topic: " + topicName);
        ServiceLoader<DeserializerFactory> loader = ServiceLoader.load(DeserializerFactory.class);
        printPackage();
        Optional<DeserializerFactory> factory = loader.findFirst();
        if (factory.isPresent()) {
            logger.info("Found deserializer: " + factory.get().getClass().getName());
            if (factory.get().supportsTopic(topicName)) {
                Deserializer<Object> deserializer = factory.get().getDeserializer(topicName);
                logger.info("Will use deserializer: " + deserializer.getClass().getSimpleName());
                return new ExternalDeserializer(deserializer);
            }
        }
        logger.info("No external deserializer found. Will use default!");
        return new DefaultMessageDeserializer();
    }

    private static void printPackage() {
        logger.info("Lookup meta-file");
        URL url = ExternalDeserializer.class.getClassLoader().getResource("kafdrop.spi.DeserializerFactory");
        logger.info("got: " + url);
    }
    @Override
    public String deserializeMessage(ByteBuffer buffer) {
        return externalDeserializer.deserialize(topic, buffer.array()).toString();
    }

}
