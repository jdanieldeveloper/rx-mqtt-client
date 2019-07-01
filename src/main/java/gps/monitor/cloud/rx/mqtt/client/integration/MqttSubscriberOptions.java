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

    private List<MqttSubscriberOption> subscriberOptions;

    private static MqttSubscriberOptions mqttSubscriberOptions;

    public static MqttSubscriberOptions getInstance() {
        if (mqttSubscriberOptions == null) {
            mqttSubscriberOptions = new MqttSubscriberOptions();
        }
        return mqttSubscriberOptions;
    }

    private MqttSubscriberOptions() {
        this.subscriberOptions = new ArrayList<>();
    }

    public MqttSubscriberOption get(int index) {
        return subscriberOptions.get(index);
    }

    public List<MqttSubscriberOption> getOptions() {
        return subscriberOptions;
    }

    public boolean addOption(MqttSubscriberOption option){
        //add index
        option.setIndex(subscriberOptions.size());
        return subscriberOptions.add(option);
    }

    public boolean removeOption(MqttSubscriberOption option){
        return subscriberOptions.removeIf(opt -> (opt.getIndex() == option.getIndex()));
    }

    public void setOptions(List<MqttSubscriberOption> mqttSubscriberOptions) {
        this.subscriberOptions = mqttSubscriberOptions;
    }

    public int size(){
        return subscriberOptions.size();
    }

    public boolean isEmpty(){
        return subscriberOptions.isEmpty();
    }

    public int[] toQos() {
        return qos;
    }

    public String[] toTopicFilters() {
        return (String[]) subscriberOptions
                    .stream()
                        .map(t -> t.getTopicFilter())
                .collect(Collectors.toList())
         .toArray();
    }

    public IMqttMessageListener[] toMessageListeners() {
        return (IMqttMessageListener[]) subscriberOptions
                    .stream()
                        .map(t -> t.getMessageListener())
                .collect(Collectors.toList())
        .toArray();
    }

    public IMqttActionListener[] toCallbacks() {
        return (IMqttActionListener[]) subscriberOptions
                    .stream()
                        .map(t -> t.getCallback())
                .collect(Collectors.toList())
        .toArray();
    }

    public Object[] toUserContexts() {
        return (Object[]) subscriberOptions
                    .stream()
                        .map(t -> t.getUserContext())
                .collect(Collectors.toList())
        .toArray();
    }
}
