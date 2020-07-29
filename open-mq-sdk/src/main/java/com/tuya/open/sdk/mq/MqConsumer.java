
package com.tuya.open.sdk.mq;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MqConsumer {

	private static final Logger	logger				= LoggerFactory.getLogger(MqConsumer.class);

	private String				serviceUrl;

	private String				accessId;

	private String				accessKey;

	private MqEnv				env					= MqEnv.PROD;

	private int					maxRedeliverCount	= 3;

	private IMessageListener	messageListener;

	public static MqConsumer build() {
		return new MqConsumer();
	}

	public MqConsumer serviceUrl(String serviceUrl) {
		this.serviceUrl = serviceUrl;
		return this;
	}

	public MqConsumer accessId(String accessId) {
		this.accessId = accessId;
		return this;
	}

	public MqConsumer accessKey(String accessKey) {
		this.accessKey = accessKey;
		return this;
	}

	public MqConsumer env(MqEnv env) {
		this.env = env;
		return this;
	}

	public MqConsumer maxRedeliverCount(int maxRedeliverCount) {
		this.maxRedeliverCount = maxRedeliverCount;
		return this;
	}

	public MqConsumer messageListener(IMessageListener messageListener) {
		this.messageListener = messageListener;
		return this;
	}

	/**
	 * Start consumer
	 * 
	 * @throws Exception
	 */
	public void start() throws Exception {
		if (serviceUrl == null || serviceUrl.trim().length() == 0) {
			throw new IllegalStateException("serviceUrl must be initialized");
		}
		if (accessId == null || accessId.trim().length() == 0) {
			throw new IllegalStateException("accessId must be initialized");
		}
		if (accessKey == null || accessKey.trim().length() == 0) {
			throw new IllegalStateException("accessKey must be initialized");
		}
		if (messageListener == null) {
			throw new IllegalStateException("messageListener must be initialized");
		}
		PulsarClient client = PulsarClient.builder().serviceUrl(serviceUrl).allowTlsInsecureConnection(true)
				.authentication(new MqAuthentication(accessId, accessKey)).build();
		Consumer consumer = client.newConsumer().topic(String.format("%s/out/%s", accessId, env.getValue()))
				.subscriptionName(String.format("%s-sub", accessId)).subscriptionType(SubscriptionType.Failover)
				.deadLetterPolicy(DeadLetterPolicy.builder().maxRedeliverCount(maxRedeliverCount).build()).subscribe();
		do {
			try {
				Message message = consumer.receive();
				Long s = System.currentTimeMillis();
				messageListener.onMessageArrived(message);
				if (MqConfigs.DEBUG) {
					logger.info("business processing cost={}", System.currentTimeMillis() - s);
				}
				consumer.acknowledge(message);
			} catch (Throwable t) {
				logger.error("error:", t);
			}
		} while (true);
	}

	public interface IMessageListener {

		void onMessageArrived(Message message) throws Exception;
	}

}
