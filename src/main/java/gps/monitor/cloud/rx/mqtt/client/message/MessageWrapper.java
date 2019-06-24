package gps.monitor.cloud.rx.mqtt.client.message;

import org.eclipse.paho.client.mqttv3.MqttMessage;

/**
 * Representa un Wrapper para un {@link MqttMessage}
 *
 * @author daniel.carvajal
 */
public class MessageWrapper extends MqttMessage {

    private String topicFilter;

    public String getTopicFilter() {
        return topicFilter;
    }

    public void setTopicFilter(String topicFilter) {
        this.topicFilter = topicFilter;
    }
}
