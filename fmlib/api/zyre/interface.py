import logging
from queue import Queue

from ropod.pyre_communicator.base_class import RopodPyre
from ropod.utils.logging.counter import ContextFilter
from fmlib.api.zyre.messages import MessageFactory


class ZyreInterface(RopodPyre):
    def __init__(self, zyre_node, logger_name='api.zyre', **kwargs):
        super().__init__(zyre_node, logger_name="RopodPyre_" + logger_name, acknowledge=kwargs.get('acknowledge', False))
        self.logger = logging.getLogger(logger_name)
        self.logger.addFilter(ContextFilter())
        self.callback_dict = dict()
        self.debug_messages = kwargs.get('debug_messages', list())
        self.publish_dict = kwargs.get('publish', dict())
        self.queue = Queue()

        self._mf = MessageFactory(kwargs.get('schema', 'unknown'))

    def register_callback(self, function, msg_type, **kwargs):
        self.logger.debug("Adding callback function %s for message type %s", function.__name__,
                          msg_type)
        self.__dict__[function.__name__] = function
        self.callback_dict[msg_type] = function.__name__

    def receive_msg_cb(self, msg_content):
        dict_msg = self.convert_zyre_msg_to_dict(msg_content)
        if dict_msg is None:
            self.logger.warning("Message is not a dictionary")
            return

        message_type = dict_msg['header']['type']
        # Ignore messages not declared in our message type
        if message_type not in self.message_types:
            return
        elif message_type in self.debug_messages:
            payload = dict_msg.get('payload')
            self.logger.debug("Received %s message, with payload %s", message_type, payload)

        self.queue.put(dict_msg)

    def publish(self, msg, **kwargs):
        try:
            msg_type = msg.get('header').get('type')
        except AttributeError:
            self.logger.error("Could not get message type from message: %s", msg, exc_info=True)
            return

        self.logger.debug("Publishing message of type %s", msg_type)

        try:
            method = self.publish_dict.get(msg_type.lower()).get('method')
        except ValueError:
            self.logger.error("No method defined for message %s", msg_type)
            return

        self.logger.debug('Using method %s to publish message', method)
        groups = kwargs.get("groups")
        getattr(self, method)(msg, groups=groups)

    def create_message(self, model):
        self.logger.debug("Creating message for model %s", model)
        return self._mf.create_message(model)

    def create_header(self, message_type, **kwargs):
        return self._mf.create_header(message_type, **kwargs)

    def create_payload_from_dict(self, payload_dict):
        return self._mf.create_payload_from_dict(payload_dict)

    def process_msgs(self):
        while not self.queue.empty():
            dict_msg = self.queue.get()
            message_type = dict_msg['header']['type']

            callback = None

            try:
                callback = self.callback_dict.get(message_type, None)
                if callback is None:
                    raise AttributeError
            except AttributeError:
                self.logger.error("No callback function found for %s messages. Callback dictionary: %s",
                                  message_type, self.callback_dict)

            try:
                if callback:
                    getattr(self, callback)(dict_msg)
            except AttributeError:
                self.logger.error("Could not execute callback %s ", callback, exc_info=True)

    def run(self):
        self.process_msgs()
        if self.acknowledge:
            self.resend_message_cb()