package gps.monitor.cloud.rx.mqtt.client.subscriber;

import gps.monitor.cloud.rx.mqtt.client.message.MessageWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by daniel.carvajal on 28-03-2019.
 */
public class TestSubcriberConsumer2 implements MessageConsumer<MessageWrapper> {

    private static final Logger logger = LoggerFactory.getLogger(TestSubcriberConsumer2.class);

    private List<MessageWrapper> messagesReceived = new ArrayList<>();

    @Override
    public void accept(MessageWrapper message) {
        logger.info("[{}] Recepcion del message[{}] desde el topico [{}]",
                    TestSubcriberConsumer2.class.getSimpleName(), new String(message.getPayload(), StandardCharsets.UTF_8), message.getTopicFilter());

        messagesReceived.add(message);
    }

    public List<MessageWrapper> getMessagesReceived() {
        return messagesReceived;
    }

    public void setMessagesReceived(List<MessageWrapper> messagesReceived) {
        this.messagesReceived = messagesReceived;
    }
}
