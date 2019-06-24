package gps.monitor.cloud.rx.mqtt.client.integration;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttMessageListener;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;


/**
 * Created by daniel.carvajal on 26-03-2019.
 */
public class MqttPublisherOptions {

    private int[] qos;

    private String[] topicFilters;

    private IMqttMessageListener[] messageListeners;

    private IMqttActionListener[] callback;

    private Object[] userContext;

    private List<MqttPublisherOption> mqttPublisherOptions;

    public MqttPublisherOptions() {
        this.mqttPublisherOptions = new ArrayList<>();
    }

    public MqttPublisherOption get(int index) {
        return mqttPublisherOptions.get(index);
    }

    public MqttPublisherOption getDefaultOption() {
        return mqttPublisherOptions.get(0);
    }

    public boolean addOption(MqttPublisherOption option){
        return mqttPublisherOptions.add(option);
    }

    public boolean removeOption(MqttPublisherOption option){
        return mqttPublisherOptions.removeIf(opt -> (opt.getIndex() == option.getIndex()));
    }

    public void setOptions(List<MqttPublisherOption> mqttPublisherOptions) {
        this.mqttPublisherOptions = mqttPublisherOptions;
    }

    public int size(){
        return mqttPublisherOptions.size();
    }

    public boolean isEmpty(){
        return mqttPublisherOptions.isEmpty();
    }

    public int[] toQos() {
        return qos;
    }

    public String[] toTopicFilters() {
        return (String[]) mqttPublisherOptions
                    .stream()
                        .map(t -> t.getTopicFilter())
                .collect(Collectors.toList())
         .toArray();
    }

    public IMqttMessageListener[] toMessageListeners() {
        return (IMqttMessageListener[]) mqttPublisherOptions
                    .stream()
                        .map(t -> t.getMessageListener())
                .collect(Collectors.toList())
        .toArray();
    }

    public IMqttActionListener[] toCallbacks() {
        return (IMqttActionListener[]) mqttPublisherOptions
                    .stream()
                        .map(t -> t.getCallback())
                .collect(Collectors.toList())
        .toArray();
    }

    public Object[] toUserContexts() {
        return (Object[]) mqttPublisherOptions
                    .stream()
                        .map(t -> t.getUserContext())
                .collect(Collectors.toList())
        .toArray();
    }
}
