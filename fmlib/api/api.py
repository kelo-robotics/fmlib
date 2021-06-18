"""This module contains the API class that allows components to receive and
send messages through the network using a variety of middlewares
"""

import logging

from fmlib.api.mqtt.interface import MQTTInterface
from fmlib.api.mqtt.messages import Message as MQTTMessage
from fmlib.api.rest.interface import RESTInterface
from fmlib.api.zyre.interface import ZyreInterface
from fmlib.api.zyre.messages import Message as ZyreMessage
from ropod.utils.logging.counter import ContextFilter


class API:
    """API object serves as a facade to different middlewares

        Args:
            middleware: a list of middleware to configure.
            The keyword arguments should contain the desired configuration
            matching the middleware listed

        Attributes:
            middleware_collection: A list of supported middlewares obtained from the config file
            config_params: A dictionary containing the parameters loaded from the config file
            _mf: An object of type MessageFactory to create message templates
    """

    def __init__(self, middleware, **kwargs):
        logger_name = kwargs.get("logger_name", "api")
        self.logger = logging.getLogger(logger_name)
        self.logger.addFilter(ContextFilter())

        self.interfaces = list()
        self.config_params = dict()
        self.middleware_collection = middleware
        self._configure(kwargs)

    def publish(self, msg,  **kwargs):
        """Publishes a message using the configured functions per middleware

        Args:
            msg: a JSON message
            **kwargs: keyword arguments to be passed to the configured functions
        """
        middleware = None

        if isinstance(msg, ZyreMessage):
            middleware = "zyre"
        elif isinstance(msg, MQTTMessage):
            middleware = "mqtt"

        interface = self.get_interface(middleware)
        if interface is None:
            return

        interface = self.__dict__[middleware]
        if hasattr(interface, "publish"):
            interface.publish(msg, **kwargs)

    def get_peer_directory(self):
        for option in self.middleware_collection:
            interface = self.__dict__[option]
            if hasattr(interface, "peer_directory"):
                return interface.peer_directory
            else:
                self.logger.warning("Option %s hast no peer_directory")

    def _configure(self, config_params):
        for option in self.middleware_collection:
            config = config_params.get(option, None)
            self.config_params[option] = config
            if config is None:
                self.logger.warning("Option %s present, but no configuration was found", option)
                self.__dict__[option] = None
                continue

            self.logger.debug("Configuring %s API", option)
            interface = None
            if option == 'zyre':
                interface = self.get_zyre_api(config)
            elif option == 'rest':
                interface = self.get_rest_api(config)
            elif option == 'mqtt':
                interface = self.get_mqtt_api(config, **config_params)

            self.__dict__[option] = interface
            self.interfaces.append(interface)

    @classmethod
    def get_zyre_api(cls, zyre_config):
        """Create an object of type ZyreInterface

        Args:
            zyre_config: A dictionary containing the API configuration

        Returns:
            A configured ZyreInterface object

        """
        zyre_api = ZyreInterface(**zyre_config)
        return zyre_api

    @classmethod
    def get_rest_api(cls, rest_config):
        """Create an object of type RESTInterface

        Args:
            rest_config: A dictionary containing the API configuration

        Returns:
            A configured RESTInterface object

        """
        return RESTInterface(**rest_config)

    @classmethod
    def get_mqtt_api(cls, mqtt_config, **kwargs):
        """Create an object of type MQTTInterface

        Args:
            mqtt_config: A dictionary containing the API configuration

        Returns:
            A configured MQTTInterface object

        """
        robots = kwargs.get("robots", dict())
        mqtt_config.update(robots=robots)
        return MQTTInterface(**mqtt_config)

    def register_callbacks(self, obj, callback_config=None):
        for option in self.middleware_collection:
            if callback_config is None:
                option_config = self.config_params.get(option, None)
            else:
                option_config = callback_config.get(option, None)

            if option_config is None:
                logging.warning("Option %s has no configuration", option)
                continue

            callbacks = option_config.get('callbacks', list())
            for callback in callbacks:
                component = callback.get('component', None)
                try:
                    function = _get_callback_function(obj, component)
                except AttributeError as err:
                    self.logger.error("%s. Skipping %s callback.", err, component)
                    continue
                self.__register_callback(option, function, **callback)

    def __register_callback(self, middleware, function, **kwargs):
        """Adds a callback function to the right middleware

        Args:
            middleware: a string specifying which middleware to use
            function: an instance of the function to call
            **kwargs:

        """
        getattr(self, middleware).register_callback(function, **kwargs)

    def start(self):
        """Start the API components
        """
        for interface in self.interfaces:
            interface.start()

    def shutdown(self):
        """Shutdown all API components
        """
        for interface in self.interfaces:
            interface.shutdown()

    def run(self):
        """Execute the API's specific methods
        """
        for interface in self.interfaces:
            interface.run()

    def get_interface(self, option):
        if option is None:
            self.logger.error("No middleware specified to create msg")
            return

        interface = self.__dict__[option]

        if interface is None:
            self.logger.error("The middleware %s does not exist", option)
            return

        return interface

    def create_message(self, model, middleware, *args):
        interface = self.get_interface(middleware)
        if interface is None:
            return

        if hasattr(interface, "create_message"):
            return interface.create_message(model, *args)

    def create_header(self, message_type, middleware=None, **kwargs):
        interface = self.get_interface(middleware)
        if interface is None:
            return

        if hasattr(interface, "create_header"):
            return interface.create_header(message_type, **kwargs)

    def create_payload_from_dict(self, payload_dict, middleware=None):
        interface = self.get_interface(middleware)
        if interface is None:
            return

        interface = self.__dict__[middleware]
        if hasattr(interface, "create_payload_from_dict"):
            return interface.create_payload_from_dict(payload_dict)


def _get_callback_function(obj, component):
    objects = component.split('.')
    child = objects.pop(0)
    if child:
        parent = getattr(obj, child)
    else:
        parent = obj
    while objects:
        child = objects.pop(0)
        parent = getattr(parent, child)

    return parent


class InterfaceBuilder:
    pass
