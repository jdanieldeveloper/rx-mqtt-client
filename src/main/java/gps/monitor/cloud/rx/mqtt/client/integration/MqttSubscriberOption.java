package gps.monitor.cloud.rx.mqtt.client.integration;

import gps.monitor.cloud.rx.mqtt.client.bus.Bus;
import gps.monitor.cloud.rx.mqtt.client.enums.MessageBusStrategy;
import gps.monitor.cloud.rx.mqtt.client.subscriber.MessageConsumer;
import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;

import java.util.List;
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

    private Bus messageSubscriberBus;

    private MessageConsumer messageSubcriptor;

    public List<MessageConsumer<Object>> messageSubscritors;

    private MessageBusStrategy subscriberBusStrategy;

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

    public Bus getMessageSubscriberBus() {
        return messageSubscriberBus;
    }

    public void setMessageSubscriberBus(Bus messageSubscriberBus) {
        this.messageSubscriberBus = messageSubscriberBus;
    }

    public MessageConsumer getMessageSubcriptor() {
        return messageSubcriptor;
    }

    public void setMessageSubcriptor(MessageConsumer messageSubcriptor) {
        this.messageSubcriptor = messageSubcriptor;
    }

    public List<MessageConsumer<Object>> getMessageSubscritors() {
        return messageSubscritors;
    }

    public void setMessageSubscritors(List<MessageConsumer<Object>> messageSubscritors) {
        this.messageSubscritors = messageSubscritors;
    }

    public MessageBusStrategy getSubscriberBusStrategy() {
        return subscriberBusStrategy;
    }

    public void setSubscriberBusStrategy(MessageBusStrategy subscriberBusStrategy) {
        this.subscriberBusStrategy = subscriberBusStrategy;
    }

    public boolean isValid(){
        boolean isValid = false;
        if((index >= 0) && (qos >= 0) && (qos <= 3) && Objects.nonNull(topicFilter)){
            isValid = true;
        }else{
            throw new IllegalArgumentException(
                    String.format("Los parametros  index[%] qos[$s] y topicFilter[%s] del subcriber option no son validas", index, qos, topicFilter));
        }
        return isValid;
    }
}
