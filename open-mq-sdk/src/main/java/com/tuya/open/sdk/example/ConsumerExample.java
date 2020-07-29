
package com.tuya.open.sdk.example;

import com.alibaba.fastjson.JSON;
import com.tuya.open.sdk.mq.AESBase64Utils;

import com.tuya.open.sdk.mq.MqConfigs;
import com.tuya.open.sdk.mq.MqConsumer;

public class ConsumerExample {

    public static void main(String[] args) throws Exception {
        String url = MqConfigs.CN_SERVER_URL;
        String accessId = "";
        String accessKey = "";

        MqConsumer mqConsumer = MqConsumer.build().serviceUrl(url).accessId(accessId).accessKey(accessKey)
                .maxRedeliverCount(3).messageListener(message -> {
                            System.out.println("---------------------------------------------------");
                            System.out.println("Message received:" + new String(message.getData()) + ",seq="
                                    + message.getSequenceId() + ",time=" + message.getPublishTime() + ",consumed time="
                                    + System.currentTimeMillis());
                            String jsonMessage = new String(message.getData());
                            MessageVO vo = JSON.parseObject(jsonMessage, MessageVO.class);
                            System.out.println("the real message data:" + AESBase64Utils.decrypt(vo.getData(), accessKey.substring(8, 24)));
                        }

                );
        mqConsumer.start();
    }
}
