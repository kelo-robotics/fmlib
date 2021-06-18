from ropod.utils.timestamp import TimeStamp


class Header(dict):
    header_ids = dict()

    def __init__(self, manufacturer, serial_number, subtopic):
        super().__init__()

        self.topic = manufacturer + "/" + serial_number + "/" + subtopic

        if self.topic not in Message.header_ids:
            Message.header_ids[self.topic] = 1
        else:
            Message.header_ids[self.topic] += 1

        self["headerId"] = Message.header_ids.get(self.topic)
        self["manufacturer"] = manufacturer
        self["serialNumber"] = serial_number
        self["timestamp"] = TimeStamp().to_str()


class Message(dict):
    header_ids = dict()

    def __init__(self, model, header):
        super().__init__()

        self.update(header)
        # self.update(model.to_dict())

        self.topic = header.topic

    @property
    def timestamp(self):
        return self.get('timestamp')


class MessageFactory:

    def create_message(self, model, manufacturer, serial_number, subtopic):
        header = Header(manufacturer, serial_number, subtopic)
        msg = Message(model, header)
        return msg

    def create_header(self, manufacturer, serial_number, subtopic):
        return Header(manufacturer, serial_number, subtopic)
