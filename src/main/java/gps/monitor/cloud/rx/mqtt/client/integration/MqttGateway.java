package gps.monitor.cloud.rx.mqtt.client.integration;

import gps.monitor.cloud.rx.mqtt.client.bus.Bus;
import gps.monitor.cloud.rx.mqtt.client.enums.MessageBusStrategy;
import gps.monitor.cloud.rx.mqtt.client.message.MessageWrapper;
import gps.monitor.cloud.rx.mqtt.client.subscriber.MessageConsumer;
import gps.monitor.cloud.rx.mqtt.client.publisher.MessagePublisher;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.eclipse.paho.client.mqttv3.persist.MqttDefaultFilePersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * Representa un MQTT Gateway
 *
 * Tiene por objetivo abstraer las operaciones con el Broker MQTT, implementa el cliente asincronico de Eclipse Paho {@link MqttAsyncClient}
 * pero capacitado para realizar publicaciones y subscripciones a traves de buses reactivos internos asegurando la
 * integridad de los mensajes entrantes y salientes. Este esta inspirado en los EIP Patterns
 *
 * @author daniel.carvajal
 * @see <a href="https://www.enterpriseintegrationpatterns.com/patterns/messaging/ObserverJmsExample.html">EIP ObserverExample</a>
 */
public class MqttGateway implements Gateway, MqttCallbackExtended {
    private String protocol;
    private String host;
    private int port;
    private String identifier;

    private boolean memoryPersistence;

    // mqtt client
    private MqttAsyncClient mqttAsyncClient;
    private MqttConnectOptions connectOptions;
    private DisconnectedBufferOptions bufferOptions;

    // subscriber
    private Bus messageSubscriberBus;
    private MessageConsumer messageSubcriptor;
    public List<Consumer<Object>> messageSubscritors;
    private MessageBusStrategy subscriberBusStrategy;
    // options
    private MqttSubscriberOptions subscriberOptions;

    // message publisher
    private Bus messagePublisherBus;
    private MessagePublisher messagePublicator;
    private MessageBusStrategy publisherBusStrategy;
    // options
    private MqttPublisherOptions publisherOptions;

    private static final Logger logger = LoggerFactory.getLogger(MqttGateway.class.getCanonicalName());

    /**
     * Conecta el cliente al broker MQTT
     *
     * @throws MqttException
     */
    public void attach() throws MqttException {
        try {
            // set default buffer options
            mqttAsyncClient.setBufferOpts(bufferOptions);

            // set callback
            mqttAsyncClient.setCallback(this);

            // connect
            mqttAsyncClient.connect(connectOptions).waitForCompletion();

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al attach del cliente Mqtt!!!", MqttGateway.class.getSimpleName());
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al attach del cliente Mqtt!!!", MqttGateway.class.getSimpleName());
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Conecta el cliente MQTT de forma segura al broker. Si el cliente no obtuvo la conexion exitosa con el broker, intentara hacerla en background hasta
     * que pueda realizarse. Esto normalmente es emplea en redes inestables
     *
     * @throws MqttException
     */
    public void saveAttach() throws MqttException {
        try {
            String serverUri = String.format("%s://%s:%s", protocol, host, port);

            // persistence type
            if(!memoryPersistence) {
                mqttAsyncClient = new MqttAsyncClient(serverUri, identifier, new MqttDefaultFilePersistence());
            }else{
                mqttAsyncClient = new MqttAsyncClient(serverUri, identifier, new MemoryPersistence());
            }

            // set default buffer options
            mqttAsyncClient.setBufferOpts(bufferOptions);

            // set callback
            mqttAsyncClient.setCallback(this);

            // connect
            mqttAsyncClient.connect(connectOptions).waitForCompletion();

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al attach del cliente Mqtt!!!", MqttGateway.class.getSimpleName());
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al attach del cliente Mqtt!!!", MqttGateway.class.getSimpleName());
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Evalua si esta conectado al broker el cliente MQTT
     *
     * @return si esta conectado el cliente
     */
    public boolean isConnected() {
        boolean isConnected = false;
        try {
            if (Objects.nonNull(mqttAsyncClient)) {
                isConnected = mqttAsyncClient.isConnected();
            }
        } catch (Exception e) {
                logger.error("[{}] no esta conectado el cliente Mqtt!!!  El cliente esta desconectado!!!", MqttGateway.class.getSimpleName());
                logger.error(e.getMessage(), e);
        }
        return isConnected;
    }

    /**
     * Desconecta el cliente MQTT del broker cerrando los recursos necesarios para salir correctamente
     *
     * @throws MqttException
     */
    public void detach() throws MqttException {
        try {
            mqttAsyncClient.disconnect();
            mqttAsyncClient.close(true);

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al detach del cliente Mqtt!!!", MqttGateway.class.getSimpleName());
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al detach del cliente Mqtt!!!", MqttGateway.class.getSimpleName());
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Des-Conecta el cliente MQTT del broker cerrando los recursos necesarios para salir correctamente
     * esperando los milisegundos entregados como parametros
     *
     * @throws MqttException
     */
    public void detach(long milis) throws MqttException, InterruptedException {
        try {
            Thread.sleep(milis);
            //
            mqttAsyncClient.disconnect();
            mqttAsyncClient.close(true);

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al detach del cliente Mqtt!!!", MqttGateway.class.getSimpleName());
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al detach del cliente Mqtt!!!", MqttGateway.class.getSimpleName());
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Re-Conecta el cliente MQTT del broker. Este metodo no funciona correctamente en algunos casos de configuracion ni en redes inestables
     * por favor use {@link MqttConnectOptions#automaticReconnect}
     *
     * @throws MqttException
     */
    public void reconnect() throws MqttException {
        try {
            mqttAsyncClient.reconnect();

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al reconnect del cliente Mqtt!!!", MqttGateway.class.getSimpleName());
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al reconnect del cliente Mqtt!!!", MqttGateway.class.getSimpleName());
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Metodo de subcripcion a todos los topicos agregados a las {@link MqttSubscriberOptions}
     *
     * @return MqttGateway subcrito a los topicos indicados en las {@link MqttSubscriberOptions}
     * @throws MqttException
     */
    public MqttGateway subscribeWithOptions() {
            if (Objects.nonNull(subscriberOptions) && !subscriberOptions.isEmpty()) {
                MqttSubscriberOption option = subscriberOptions.getDefaultOption();
                if (Objects.nonNull(option) && option.isValid()) {
                    try {
                        subscribe(option.getTopicFilter(), option.getQos());
                    } catch (MqttException e) {
                        logger.error("Error al subsribir al  topico[{}] host[{}] !!!", option.getTopicFilter(),  mqttAsyncClient.getServerURI());
                        logger.error(e.getMessage(), e);

                    } catch (Exception e) {
                        logger.error("Error al subsribir al  topico[{}] host[{}]!!!",  option.getTopicFilter(), mqttAsyncClient.getServerURI());
                        logger.error(e.getMessage(), e);
                    }
                }
                //
                logger.info("Se ha subcrito al topico[{}] host[{}] correctamente!!!", mqttAsyncClient.getServerURI(), option.getTopicFilter());

            } else {
                logger.warn("[{}] Las optiones de subcripcion al cliente Mqtt no deberian ser nulas!!!", MqttAsyncClient.class.getSimpleName());
            }
        return this;
    }

    //TODO metodo subcriptor por subcriber option


    /**
     * Metodo que publica un mensaje en formato {@link String} a todos los topicos agregados a las {@link MqttPublisherOptions}
     *
     * @throws MqttException
     */
    public void publishWithOptions(String payload) throws MqttException {
        if (Objects.nonNull(publisherOptions) && !publisherOptions.isEmpty()) {
            MqttPublisherOption option = publisherOptions.getDefaultOption();
            if (Objects.nonNull(option) && option.isValid()) {
               MessageWrapper mqttMessage = new MessageWrapper();
                // set options
                //mqttMessage.setId();
                mqttMessage.setQos(option.getQos());
                mqttMessage.setRetained(option.isRetained());
                mqttMessage.setPayload(payload.getBytes());
                //
                mqttMessage.setTopicFilter(option.getTopicFilter());
                //
                messagePublisherBus.handle(mqttMessage);
            }
            //
            logger.info("Se ha publicado el mensaje[{}] al host[{}] al topico [{}] correctamente!!!", payload, mqttAsyncClient.getServerURI(), option.getTopicFilter());

        } else {
            logger.warn("[{}] Las optiones de publicacion al cliente Mqtt no deberian ser nulas!!!", MqttAsyncClient.class.getSimpleName());

        }
    }

    //TODO un publicador por publisher option

    /**
     * Wrapper del metodo nativo {@link MqttAsyncClient#subscribe(String, int)}
     *
     * @param topicFilter
     * @param qos
     * @throws MqttException
     */
    public void subscribe(String topicFilter, int qos) throws MqttException {
        try {
            mqttAsyncClient.subscribe(topicFilter, qos);

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al subscribir al topico[{}] del cliente Mqtt!!!", MqttGateway.class.getSimpleName(), topicFilter);
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al subscribir al topico[{}] del cliente Mqtt!!!", MqttGateway.class.getSimpleName(), topicFilter);
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Wrapper del metodo nativo {@link MqttAsyncClient#subscribe(String, int, IMqttMessageListener)}
     *
     * @param topicFilter
     * @param qos
     * @param messageListener
     * @throws MqttException
     */
    public void subscribe(String topicFilter, int qos, IMqttMessageListener messageListener) throws MqttException {
        try {
            IMqttToken token = mqttAsyncClient.subscribe(topicFilter, qos, messageListener);

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al subscribir al topico[{}] del cliente Mqtt!!!", MqttGateway.class.getSimpleName(), topicFilter);
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al subscribir al topico[{}] del cliente Mqtt!!!", MqttGateway.class.getSimpleName(), topicFilter);
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Wrapper del metodo nativo {@link MqttAsyncClient#subscribe(String, int, Object, IMqttActionListener)}
     *
     * @param topicFilter
     * @param qos
     * @param userContext
     * @param callback
     * @throws MqttException
     */
    public void subscribe(String topicFilter, int qos, Object userContext, IMqttActionListener callback) throws MqttException {
        try {
            mqttAsyncClient.subscribe(topicFilter, qos, userContext, callback);

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al subscribir al topico[{}] del cliente Mqtt!!!", MqttGateway.class.getSimpleName(), topicFilter);
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al subscribir al topico[{}] del cliente Mqtt!!!", MqttGateway.class.getSimpleName(), topicFilter);
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Wrapper del metodo nativo {@link MqttAsyncClient#subscribe(String, int, Object, IMqttActionListener, IMqttMessageListener)}
     *
     * @param topicFilter
     * @param qos
     * @param userContext
     * @param callback
     * @param messageListener
     * @throws MqttException
     */
    public void subscribe(String topicFilter, int qos, Object userContext, IMqttActionListener callback, IMqttMessageListener messageListener) throws MqttException {
        try {
            mqttAsyncClient.subscribe(topicFilter, qos, userContext, callback);

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al subscribir al topico[{}] del cliente Mqtt!!!", MqttGateway.class.getSimpleName(), topicFilter);
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al subscribir al topico[{}] del cliente Mqtt!!!", MqttGateway.class.getSimpleName(), topicFilter);
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Publica un mensaje con los parametros indicados en  {@link MessageWrapper}
     *
     * @param message
     * @throws MqttException
     */
    public void publish(MessageWrapper message) throws MqttException {
        try {
            messageSubscriberBus.handle(message);

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al publicar el mensaje [{}] en el topico [{}]",
                        MqttGateway.class.getSimpleName(), new String(message.getPayload(), StandardCharsets.UTF_8), message.getTopicFilter());
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Wrapper del metodo nativo {@link MqttAsyncClient#publish(String, byte[], int, boolean)}
     *
     * @param topic
     * @param payload
     * @param qos
     * @param retained
     * @throws MqttException
     */
    public void publish(String topic, byte[] payload, int qos, boolean retained) throws MqttException {
        try {
            mqttAsyncClient.publish(topic, payload, qos, retained);

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al publicar el mensaje [{}] en el topico [{}]",
                        MqttGateway.class.getSimpleName(), new String(payload, StandardCharsets.UTF_8), topic);
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al publicar el mensaje [{}] en el topico [{}]",
                        MqttGateway.class.getSimpleName(), new String(payload, StandardCharsets.UTF_8), topic);
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Wrapper del metodo nativo {@link MqttAsyncClient#publish(String, MqttMessage, Object, IMqttActionListener)}
     *
     * @param topic
     * @param payload
     * @param qos
     * @param retained
     * @param userContext
     * @param callback
     * @throws MqttException
     */
    public void publish(String topic, byte[] payload, int qos, boolean retained, Object userContext, IMqttActionListener callback) throws MqttException {
        try {
            mqttAsyncClient.publish(topic, payload, qos, retained, userContext, callback);

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al publicar el mensaje [{}] en el topico [{}]",
                        MqttGateway.class.getSimpleName(), new String(payload, StandardCharsets.UTF_8), topic);
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al publicar el mensaje [{}] en el topico [{}]",
                        MqttGateway.class.getSimpleName(), new String(payload, StandardCharsets.UTF_8), topic);
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Wrapper del metodo nativo {@link MqttAsyncClient#publish(String, MqttMessage)}
     *
     * @param topic
     * @param message
     * @throws MqttException
     */
    public void publish(String topic, MqttMessage message) throws MqttException {
        try {
            mqttAsyncClient.publish(topic, message);

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al publicar el mensaje [{}] en el topico [{}]", MqttGateway.class.getSimpleName(), message.toString(), topic);
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al publicar el mensaje [{}] en el topico [{}]", MqttGateway.class.getSimpleName(), message.toString(), topic);
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Wrapper del metodo nativo {@link MqttAsyncClient#publish(String, MqttMessage, Object, IMqttActionListener)}
     *
     * @param topic
     * @param message
     * @param userContext
     * @param callback
     * @throws MqttException
     */
    public void publish(String topic, MqttMessage message, Object userContext, IMqttActionListener callback) throws MqttException {
        try {
            mqttAsyncClient.publish(topic, message, userContext, callback);

        } catch (MqttException e) {
            logger.error("[{}] Se ha producido un error al publicar el mensaje [{}] en el topico [{}]", MqttGateway.class.getSimpleName(), message.toString(), topic);
            logger.error(e.getMessage(), e);

            throw e;

        } catch (Exception e) {
            logger.error("[{}] Se ha producido un error al publicar el mensaje [{}] en el topico [{}]", MqttGateway.class.getSimpleName(), message.toString(), topic);
            logger.error(e.getMessage(), e);

            throw e;
        }
    }

    /**
     * Recibe los mensajes desde los topicos subcritos por el {@link MqttGateway} y luego los publica en un {@link Bus}
     *
     * @param topic
     * @param mqttMessage
     */
    @Override
    public void messageArrived(String topic, MqttMessage mqttMessage) {
        if (!mqttMessage.isDuplicate()) {
            MessageWrapper message = new MessageWrapper();
            //message.setId();
            message.setTopicFilter(topic);
            message.setPayload(mqttMessage.getPayload());
            message.setQos(mqttMessage.getQos());
            message.setRetained(mqttMessage.isRetained());
            //
            messageSubscriberBus.handle(message);
        }else{
            if (logger.isDebugEnabled()) {
                logger.debug("[{}] Se ha detectado un mensaje[{}] duplicado!!! ", MqttGateway.class.getSimpleName(), mqttMessage.toString());
            }
        }
    }

    /**
     * Evento del {@link MqttAsyncClient#mqttCallback} que se produce cuando la conexion se realiza satifactoriamente
     *
     * @param b
     * @param host
     */
    @Override
    public void connectComplete(boolean b, String host) {
        logger.info("[{}] Se ha conectado completamente el cliente al host [{}]", MqttGateway.class.getSimpleName(), host);
        //
        subscribeWithOptions();

    }

    /**
     * Evento del {@link MqttAsyncClient#mqttCallback} que se produce cuando se pierde la conexion con el broker
     *
     * @param mqtte
     */
    @Override
    public void connectionLost(Throwable mqtte) {
        try {
            logger.info("[{}] Mqtt perdio conexion con el host [{}]", MqttGateway.class.getSimpleName(), mqttAsyncClient.getServerURI());
            if (logger.isDebugEnabled()) {
                logger.debug(mqtte.getMessage(), mqtte);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * Evento del {@link MqttAsyncClient#mqttCallback} que se produce cuando se envia un mensaje y
     * llega su ask de confirmacion
     *
     * @param mqttDeliveryToken
     */
    @Override
    public void deliveryComplete(IMqttDeliveryToken mqttDeliveryToken) {
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getIdentifier() {
        return identifier;
    }

    public void setIdentifier(String identifier) {
        this.identifier = identifier;
    }

    public boolean isMemoryPersistence() {
        return memoryPersistence;
    }

    public void setMemoryPersistence(boolean memoryPersistence) {
        this.memoryPersistence = memoryPersistence;
    }

    public MqttAsyncClient getMqttAsyncClient() {
        return mqttAsyncClient;
    }

    public void setMqttAsyncClient(MqttAsyncClient mqttAsyncClient) {
        this.mqttAsyncClient = mqttAsyncClient;
    }

    public MqttConnectOptions getConnectOptions() {
        return connectOptions;
    }

    public void setConnectOptions(MqttConnectOptions connectOptions) {
        this.connectOptions = connectOptions;
    }

    public DisconnectedBufferOptions getBufferOptions() {
        return bufferOptions;
    }

    public void setBufferOptions(DisconnectedBufferOptions bufferOptions) {
        this.bufferOptions = bufferOptions;
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

    public List<Consumer<Object>> getMessageSubscritors() {
        return messageSubscritors;
    }

    public void setMessageSubscritors(List<Consumer<Object>> messageSubscritors) {
        this.messageSubscritors = messageSubscritors;
    }

    public MessageBusStrategy getSubscriberBusStrategy() {
        return subscriberBusStrategy;
    }

    public void setSubscriberBusStrategy(MessageBusStrategy subscriberBusStrategy) {
        this.subscriberBusStrategy = subscriberBusStrategy;
    }

    public MqttSubscriberOptions getSubscriberOptions() {
        return subscriberOptions;
    }

    public void setSubscriberOptions(MqttSubscriberOptions subscriberOptions) {
        this.subscriberOptions = subscriberOptions;
    }

    public Bus getMessagePublisherBus() {
        return messagePublisherBus;
    }

    public void setMessagePublisherBus(Bus messagePublisherBus) {
        this.messagePublisherBus = messagePublisherBus;
    }

    public MessagePublisher getMessagePublicator() {
        return messagePublicator;
    }

    public void setMessagePublicator(MessagePublisher messagePublicator) {
        this.messagePublicator = messagePublicator;
    }

    public MessageBusStrategy getPublisherBusStrategy() {
        return publisherBusStrategy;
    }

    public void setPublisherBusStrategy(MessageBusStrategy publisherBusStrategy) {
        this.publisherBusStrategy = publisherBusStrategy;
    }

    public MqttPublisherOptions getPublisherOptions() {
        return publisherOptions;
    }

    public void setPublisherOptions(MqttPublisherOptions publisherOptions) {
        this.publisherOptions = publisherOptions;
    }
}
