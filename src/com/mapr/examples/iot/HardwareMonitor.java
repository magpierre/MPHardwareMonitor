/*

   Copyright 2019 MapR Technologies

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/

package com.mapr.examples.iot;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import oshi.json.SystemInfo;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.json.simple.*;
import org.apache.commons.cli.*;

public class HardwareMonitor {
    public static void main(String[] args) {
        HelpFormatter formatter = new HelpFormatter();
        Options options = new Options();
        try {
            Option brokerOption = new Option("b", "broker", true, "MQTT Broker address");
            brokerOption.setRequired(false);
            options.addOption(brokerOption);
            Option topicOption = new Option("t", "topic", true, "MQTT Topic");
            topicOption.setRequired(false);
            options.addOption(topicOption);
            Option clientIDOption = new Option("c", "clientID", true, "Client ID");
            clientIDOption.setRequired(false);
            options.addOption(clientIDOption);
            Option SleepOption = new Option("s", "sleep", true, "Sleep between sends");
            SleepOption.setRequired(false);
            options.addOption(SleepOption);
            Option RetrySleepOption = new Option("r", "retry-sleep", true, "Sleep between retries");
            RetrySleepOption.setRequired(false);
            options.addOption(RetrySleepOption);
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, args);
            String brokerURL = cmd.getOptionValue("broker", "tcp://localhost:1883");
            String topicName = cmd.getOptionValue("topic", "mac/HWInfo");
            String clientID = cmd.getOptionValue("clientID", "HWSensor");
            String SleepValue = cmd.getOptionValue("sleep", "1000");
            String RetrySleepValue = cmd.getOptionValue("retry-sleep", "30000");
            try {
                while (1 == 1) {
                    try {
                        SystemInfo s = new SystemInfo();
                        String hwTopic = topicName;
                        int qos = 1;
                        String broker = brokerURL;
                        String clientId = clientID;
                        MemoryPersistence persistence = new MemoryPersistence();
                        MqttClient sampleClient = new MqttClient(broker, clientId, persistence);
                        MqttConnectOptions connOpts = new MqttConnectOptions();
                        connOpts.setCleanSession(true);
                        System.out.println("Connecting to broker: " + broker);
                        sampleClient.connect(connOpts);
                        System.out.println("Connected");
                        JSONParser jsonParser = new JSONParser();
                        while (1 == 1) {
                            String new_message = null;
                            try {
                                JSONObject o = (JSONObject) jsonParser.parse(s.toCompactJSON());
                                o.put("generation_ts", new java.util.Date().getTime());
                                new_message = o.toString();
                            } catch (ParseException e) {
                                System.out.println(e.getLocalizedMessage());
                                return;
                            }
                            MqttMessage message = new MqttMessage(new_message.getBytes());
                            System.out.println("Publishing message: " + message.toString());
                            message.setQos(qos);
                            sampleClient.publish(hwTopic, message);
                            Thread.sleep(Integer.parseInt(SleepValue));
                        }
                    } catch (MqttException me) {
                        System.out.println("reason: " + me.getReasonCode());
                        System.out.println("msg: " + me.getMessage());
                        System.out.println("loc: " + me.getLocalizedMessage());
                        System.out.println("cause: " + me.getCause());
                        System.out.println("excep: " + me);
                        me.printStackTrace();
                    } catch (InterruptedException me) {
                        System.out.println("msg: " + me.getMessage());
                        System.out.println("loc: " + me.getLocalizedMessage());
                        System.out.println("cause: " + me.getCause());
                        System.out.println("excep: " + me);
                        me.printStackTrace();
                    }
                    Thread.sleep(Integer.parseInt(RetrySleepValue));
                }
            } catch (InterruptedException me) {
                System.out.println("msg: " + me.getMessage());
                System.out.println("loc: " + me.getLocalizedMessage());
                System.out.println("cause: " + me.getCause());
                System.out.println("excep: " + me);
                me.printStackTrace();
            }
        } catch (org.apache.commons.cli.ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("utility-name", options);

            System.exit(1);
        }


    }
}
