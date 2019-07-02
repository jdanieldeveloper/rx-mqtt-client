package gps.monitor.cloud.rx.mqtt.client.listener;

import gps.monitor.cloud.rx.mqtt.client.bus.Bus;
import gps.monitor.cloud.rx.mqtt.client.converter.Converters;
import gps.monitor.cloud.rx.mqtt.client.integration.MqttGateway;
import gps.monitor.cloud.rx.mqtt.client.message.MessageWrapper;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultMqttListener implements IMqttMessageListener {

    private Bus messageSubscriberBus;

    private static final Logger logger = LoggerFactory.getLogger(DefaultMqttListener.class);

    @Override
    public void messageArrived(String mqttTopic, MqttMessage mqttMessage) throws Exception {
        try {
            if (!mqttMessage.isDuplicate()) {
                MessageWrapper message = new MessageWrapper();
                //message.setId();
                message.setTopicFilter(mqttTopic);
                message.setPayload(mqttMessage.getPayload());
                message.setQos(mqttMessage.getQos());
                message.setRetained(mqttMessage.isRetained());

                messageSubscriberBus.handle(message);

            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("[{}] Se ha detectado un mensaje[{}] duplicado!!! ", MqttGateway.class.getSimpleName(), Converters.bytesToString(mqttMessage.getPayload()));
                }
            }
        }catch (Exception e){
            logger.error("[{}] Se ha producido un error al recibir el mensaje [{}] en el topico [{}]",
                    MqttGateway.class.getSimpleName(), Converters.bytesToString(mqttMessage.getPayload()), mqttTopic);
            logger.error(e.getMessage(), e);
        }
    }

    public Bus getMessageSubscriberBus() {
        return messageSubscriberBus;
    }

    public void setMessageSubscriberBus(Bus messageSubscriberBus) {
        this.messageSubscriberBus = messageSubscriberBus;
    }
}
