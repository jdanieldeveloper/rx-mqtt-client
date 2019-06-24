package gps.monitor.cloud.rx.mqtt.client.conf.impl;

import gps.monitor.cloud.rx.mqtt.client.conf.Configuration;
import io.moquette.interception.AbstractInterceptHandler;
import io.moquette.interception.InterceptHandler;
import io.moquette.interception.messages.InterceptPublishMessage;
import io.moquette.server.Server;
import io.moquette.server.config.ClasspathResourceLoader;
import io.moquette.server.config.IConfig;
import io.moquette.server.config.IResourceLoader;
import io.moquette.server.config.ResourceLoaderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Objects;


import static java.nio.charset.StandardCharsets.UTF_8;


/**
 * Levanta un MqttServer Moquette embebido stand alone
 *
 * @author daniel.carvajal
 * @see <a href="https://projects.eclipse.org/proposals/moquette-mqtt">Eclipse Moquette MQTT</a>
 */
public class MqttServerConfigApp implements Configuration {

    private Server mqttBroker;

    private static final Logger logger = LoggerFactory.getLogger(MqttServerConfigApp.class);

    /**
     * Configura el server
     */
    @Override
    public void configure() {
        mqttServer();
    }


    /**
     * Inicializa el server
     */
    public void mqttServer() {
        try {
            logger.info("Init service   ............... [{}]", Server.class.getSimpleName());
            IResourceLoader classpathLoader = new ClasspathResourceLoader();
            //
            final IConfig classPathConfig = new ResourceLoaderConfig(classpathLoader);
            mqttBroker = new Server();
            List<? extends InterceptHandler> userHandlers = Collections.singletonList(new PublisherListener());
            mqttBroker.startServer(classPathConfig, userHandlers);
            //
            logger.info("Estatus service ............... [{}] [{}]",Server.class.getSimpleName(), "UP");

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logger.error("Estatus service ............... [{}] [{}]", Server.class.getSimpleName(), "DOWN");

        }
    }

    /**
     * Destruye los recursos del server
     */
    @Override
    public void destroy() {
        try {
            if (Objects.nonNull(mqttBroker)) {
                mqttBroker.stopServer();
            }
            logger.info("Estatus service ............... [{}] [{}]", Server.class.getSimpleName(), "DOWN");

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logger.error("Estatus service ............... [{}] [{}]", Server.class.getSimpleName(), "DOWN");
        }
    }

    /**
     * Listener que permite saber que mensaje a llegado a un topico determinado. Este se utiliza con fines de trazabilidad del mensaje
     */
    private class PublisherListener extends AbstractInterceptHandler {

        @Override
        public String getID() {
            return "EmbeddedLauncherPublishListener";
        }

        @Override
        public void onPublish(InterceptPublishMessage message) {
            final String decodedPayload = new String(message.toString().getBytes(), UTF_8);
            logger.error("Received on topic: [{}] | content: [{}]", message.getTopicName(), decodedPayload);
        }
    }
}
