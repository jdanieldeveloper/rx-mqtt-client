package gps.monitor.cloud.rx.mqtt.client.integration;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Representa Opciones de Subcripcion a los topicos
 *
 * @author daniel.carvajal
 *
 */
public class MqttSubscriberOptions {

    private int[] qos;

    private String[] topicFilters;

    private IMqttMessageListener[] messageListeners;

    private IMqttActionListener[] callback;

    private Object[] userContext;

    private List<MqttSubscriberOption> mqttSubscriberOptions;

    public MqttSubscriberOptions() {
        this.mqttSubscriberOptions = new ArrayList<>();
    }

    public MqttSubscriberOption get(int index) {
        return mqttSubscriberOptions.get(index);
    }

    public MqttSubscriberOption getDefaultOption() {
        return mqttSubscriberOptions.get(0);
    }

    public boolean addOption(MqttSubscriberOption option){
        return mqttSubscriberOptions.add(option);
    }

    public boolean removeOption(MqttSubscriberOption option){
        return mqttSubscriberOptions.removeIf(opt -> (opt.getIndex() == option.getIndex()));
    }

    public void setOptions(List<MqttSubscriberOption> mqttSubscriberOptions) {
        this.mqttSubscriberOptions = mqttSubscriberOptions;
    }

    public int size(){
        return mqttSubscriberOptions.size();
    }

    public boolean isEmpty(){
        return mqttSubscriberOptions.isEmpty();
    }

    public int[] toQos() {
        return qos;
    }

    public String[] toTopicFilters() {
        return (String[]) mqttSubscriberOptions
                    .stream()
                        .map(t -> t.getTopicFilter())
                .collect(Collectors.toList())
         .toArray();
    }

    public IMqttMessageListener[] toMessageListeners() {
        return (IMqttMessageListener[]) mqttSubscriberOptions
                    .stream()
                        .map(t -> t.getMessageListener())
                .collect(Collectors.toList())
        .toArray();
    }

    public IMqttActionListener[] toCallbacks() {
        return (IMqttActionListener[]) mqttSubscriberOptions
                    .stream()
                        .map(t -> t.getCallback())
                .collect(Collectors.toList())
        .toArray();
    }

    public Object[] toUserContexts() {
        return (Object[]) mqttSubscriberOptions
                    .stream()
                        .map(t -> t.getUserContext())
                .collect(Collectors.toList())
        .toArray();
    }
}
