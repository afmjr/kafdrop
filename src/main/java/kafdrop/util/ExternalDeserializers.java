package kafdrop.util;

import kafdrop.config.MessageFormatConfiguration;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.vermiculus.kafdrop.spi.ExternalDeserializerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceConfigurationError;
import java.util.ServiceLoader;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * This class managed the creation of ExternalDeserializer
 */
public class ExternalDeserializers {
    private static final Logger logger = LoggerFactory.getLogger(ExternalDeserializers.class);
    private static final Map<String, MessageDeserializer> deserializerCache = new HashMap<>();

    private ExternalDeserializers() {
        // Intentionally left empty
    }

    /**
     * Use the configuration to locate the folder where to look for jar files from.
     * Create an URLClassLoader for this location and start lookup providers of the
     * ExternalDeserializerFactory service.
     *
     * @param topicName the name of the topic to look up a deserializer for.
     * @param properties the message format configuration properties.
     * @return a message deserializer to use for the given topic name.
     */
    public static MessageDeserializer lookup(
            String topicName,
            MessageFormatConfiguration.MessageFormatProperties properties) {
        return deserializerCache.computeIfAbsent(topicName,
                topic -> internalLookup(topic, properties)
                        .map(serdes -> new ExternalDeserializer(serdes, topicName))
                        .map(MessageDeserializer.class::cast)
                        .orElseGet(DefaultMessageDeserializer::new));
    }

    private static Optional<Deserializer<Object>> internalLookup(
            String topicName,
            MessageFormatConfiguration.MessageFormatProperties properties) {
        logger.info("Lookup external deserializer for topic: {} in {}",
                topicName, properties.getExternalFormatLocation());
        Deserializer<Object> deserializer = null;
        try {
            ServiceLoader<ExternalDeserializerFactory> loader =
                    ServiceLoader.load(
                            ExternalDeserializerFactory.class,
                            getCustomLoader(properties.getExternalFormatLocation()));
            if (loader.iterator().hasNext()) {
                ExternalDeserializerFactory factory = loader.iterator().next();
                deserializer = factory.getDeserializer(topicName);
                logger.info("Using Deserializer ({})", deserializer.getClass().getName());
            } else {
                logger.info("No Deserializer found, falling back to default!");
            }
        } catch (ServiceConfigurationError e) {
            logger.warn("Unable to load deserializer", e);
        }
        return Optional.ofNullable(deserializer);
    }

    private static ClassLoader getCustomLoader(String externalFormatLocation) {
        try {
            return new URLClassLoader(getUrls(externalFormatLocation),
                    ExternalDeserializerFactory.class.getClassLoader());
        } catch (IOException e) {
            logger.error("Exception thrown", e);
            System.exit(1);
        }
        return null;
    }

    /*
     * Returns an array of URLS for all jar files found in the location.
     */
    private static URL[] getUrls(String externalFormatLocation) throws IOException {
        try {
            List<URL> urlList;
            try (Stream<Path> stream = Files.list(Paths.get(externalFormatLocation))) {
                urlList = stream
                        .filter(path -> !Files.isDirectory(path))
                        .filter(ExternalDeserializers::isJarFile)
                        .map(Path::toUri)
                        .map(ExternalDeserializers::toOptionalUrl)
                        .filter(Optional::isPresent)
                        .map(Optional::get)
                        .collect(Collectors.toList());
            }
            return urlList.toArray(new URL[0]);
        }
        catch (IOException e) {
            logger.error("Unable to load deserializers from {}", externalFormatLocation);
            throw e;
        }
    }

    private static boolean isJarFile(Path path) {
        return path.toString().endsWith(".jar");
    }

    private static Optional<URL> toOptionalUrl(URI uri) {
        try {
            return Optional.of(uri.toURL());
        } catch (MalformedURLException e) {
            logger.error("Unable to handle: " + uri, e);
            return Optional.empty();
        }
    }
}
