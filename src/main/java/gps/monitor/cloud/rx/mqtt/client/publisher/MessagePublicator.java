package gps.monitor.cloud.rx.mqtt.client.publisher;

import gps.monitor.cloud.rx.mqtt.client.integration.MqttGateway;
import gps.monitor.cloud.rx.mqtt.client.message.MessageWrapper;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementacion de {@link MessagePublisher} que tiene como objetivo consumir un {@link MessageWrapper} y
 * publicarlo en un topico MQTT, segun cual sea la configuracion del mensaje
 *
 * @author daniel.carvajal
 */
public class MessagePublicator implements MessagePublisher<MessageWrapper> {

    private MqttGateway mqttGateway;

    private static final Logger logger = LoggerFactory.getLogger(MessagePublicator.class);

    /**
     * Consume un {@link MessageWrapper} y lo publica en un topico determinado segun sus parametros
     *
     * @param message
     */
    @Override
    public void accept(MessageWrapper message) {
        try {
            if(logger.isDebugEnabled()) {
                logger.debug("[{}] Publicando mensaje [{}] en el topico [{}]!!!",
                        MessagePublicator.class.getSimpleName(), new String(message.getPayload()), message.getTopicFilter());
            }
            mqttGateway.publish(message.getTopicFilter(), message.getPayload(), message.getQos(), message.isRetained());

        } catch (MqttException e) {
            logger.error(e.getMessage(), e);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public MqttGateway getMqttGateway() {
        return mqttGateway;
    }

    public void setMqttGateway(MqttGateway mqttGateway) {
        this.mqttGateway = mqttGateway;
    }
}
