import json
import logging
from queue import Queue

import paho.mqtt.client as mqtt
from ropod.utils.logging.counter import ContextFilter


class MQTTInterface:
    def __init__(self, client_id, logger_name="api.mqtt", **kwargs):
        self.logger = logging.getLogger(logger_name)
        self.logger.addFilter(ContextFilter())

        self.host = kwargs.get("host", "127.0.0.1")
        self.port = kwargs.get("port", 1883)
        self.user = kwargs.get("user")
        self.password = kwargs.get("password")
        self.client = mqtt.Client(client_id=client_id)

        self.subtopics = kwargs.get("subtopics", list())
        self.queue = Queue()
        self.callback_dict = dict()

        self._connected = False
        self.connect()
        self._configure(**kwargs)

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            self.logger.info("Connected to MQTT Broker!")
            self._connected = True
        else:
            self.logger.error("Failed to connect, return code %s\n", rc)
            self._connected = False

    def connect(self):
        if self._connected:
            return
        if self.user:
            self.client.username_pw_set(username=self.user, password=self.password)
        self.client.on_connect = self.on_connect
        self.client.connect(self.host, self.port)

    def register_callback(self, function, subtopic, **kwargs):
        self.logger.debug("Adding callback function %s for subtopic %s", function.__name__,
                          subtopic)
        self.__dict__[function.__name__] = function
        self.callback_dict[subtopic] = function.__name__

    def on_message(self, client, userdata, msg):

        if any(subtopic in msg.topic for subtopic in self.subtopics):
            self.queue.put(msg)

    @property
    def connected(self):
        if not self._connected:
            self.connect()

        return self._connected

    def _configure(self, **kwargs):
        robots = kwargs.get("robots")
        all_serial_numbers = [robot_info.get("serial_number") for robot_id, robot_info in robots.items()]

        for subscriber in kwargs.get("subscribers", list()):
            serial_numbers = subscriber.get("serial_numbers")
            if not serial_numbers:  # The list "serial numbers" could be an empty list
                serial_numbers = all_serial_numbers
            for serial_number in serial_numbers:
                self.add_subscriber(subscriber["manufacturer"], serial_number, subscriber["subtopic"])

        self.client.on_message = self.on_message

    def add_subscriber(self, manufacturer, serial_number, subtopic):
        topic = manufacturer + "/" + serial_number + "/" + subtopic
        self.logger.debug("Adding subscriber for topic %s", topic)
        self.client.subscribe(topic)

    def publish(self, msg, topic):
        self.client.publish(topic, msg)

    def process_msgs(self):
        while not self.queue.empty():
            msg = self.queue.get()
            subtopic = msg.topic.split("/")[-1]
            msg_str = msg.payload.decode('utf-8')
            msg_dict = json.loads(msg_str)

            callback = None

            try:
                callback = self.callback_dict.get(subtopic, None)
                if callback is None:
                    raise AttributeError
            except AttributeError:
                self.logger.error("No callback function found for %s subtopic. Callback dictionary: %s",
                                  subtopic, self.callback_dict)

            try:
                if callback:
                    getattr(self, callback)(msg_dict)
            except AttributeError:
                self.logger.error("Could not execute callback %s ", callback, exc_info=True)

    def start(self):
        self.client.loop_start()

    def run(self):
        self.process_msgs()

    def shutdown(self):
        self.client.disconnect()
        self.client.loop_stop()
