package gps.monitor.cloud.rx.mqtt.client.integration;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;

import java.util.Objects;

/**
 * Created by daniel.carvajal on 26-03-2019.
 */
public class MqttSubscriberOption {

    private int index;

    private int qos;

    private String topicFilter;

    private Object userContext;

    private IMqttActionListener callback;

    private IMqttMessageListener messageListener;

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getQos() {
        return qos;
    }

    public void setQos(int qos) {
        this.qos = qos;
    }

    public String getTopicFilter() {
        return topicFilter;
    }

    public void setTopicFilter(String topicFilter) {
        this.topicFilter = topicFilter;
    }

    public Object getUserContext() {
        return userContext;
    }

    public void setUserContext(Object userContext) {
        this.userContext = userContext;
    }

    public IMqttActionListener getCallback() {
        return callback;
    }

    public void setCallback(IMqttActionListener callback) {
        this.callback = callback;
    }

    public IMqttMessageListener getMessageListener() {
        return messageListener;
    }

    public void setMessageListener(IMqttMessageListener messageListener) {
        this.messageListener = messageListener;
    }

    public boolean isValid(){
        boolean isValid = false;
        if((index >= 0) && (qos >= 0) && (qos <= 3) && Objects.nonNull(topicFilter) && Objects.nonNull(messageListener)){
            isValid = true;
        }
        return isValid;
    }
}
