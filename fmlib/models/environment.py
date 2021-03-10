from datetime import timedelta

import dateutil.parser
import numpy as np
from fmlib.utils.messages import Document
from pymodm import EmbeddedMongoModel, fields
from ropod.utils.timestamp import TimeStamp


class Timepoint(EmbeddedMongoModel):
    utc_time = fields.DateTimeField()
    timezone_offset = fields.FloatField()

    @classmethod
    def create_new(cls, **kwargs):
        utc_time = kwargs.get("utc_time", TimeStamp().to_datetime())
        timezone_offset = kwargs.get("timezone_offset", utc_time.utcoffset().total_seconds()/60)
        timepoint = cls(utc_time=utc_time, timezone_offset=timezone_offset)
        return timepoint

    def postpone(self, delta, resolution='seconds'):
        try:
            _res = {'hours': 3600, 'minutes': 60, 'seconds': 1}.get(resolution)
            delta = delta * _res
            self.utc_time += timedelta(seconds=delta)
        except NameError:
            print("Invalid resolution")

    def __str__(self):
        return f" UTC time {self.utc_time.isoformat()} \t" \
               f" Timezone offset {self.timezone_offset}"

    def __lt__(self, other):
        return self.utc_time < other.utc_time

    def __add__(self, delta):
        utc_time = self.utc_time + delta
        return self.create_new(utc_time=utc_time, timezone_offset=self.timezone_offset)

    def __sub__(self, other):
        delta = self.utc_time - other.utc_time
        return delta

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr["utc_time"] = self.utc_time.isoformat()
        return dict_repr

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        utc_time = dateutil.parser.parse(document.pop("utc_time"))
        utc_time = utc_time.replace(microsecond=0)
        document['utc_time'] = utc_time
        timepoint = cls.from_document(document)
        return timepoint


class Position(EmbeddedMongoModel):

    x = fields.FloatField()
    y = fields.FloatField()
    theta = fields.FloatField(default=0)
    name = fields.CharField()
    map = fields.CharField()

    class Meta:
        ignore_unknown_fields = True
        meta_model = "position"

    def __eq__(self, other):
        if not isinstance(other, Position):
            return False
        return (self.map == other.map
                and self.x == other.x
                and self.y == other.y
                and self.theta == other.theta)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        to_print = "[{}, {}, {}]".format(self.x, self.y, self.theta)
        if self.map:
            to_print += f" map: {self.map}"
        if self.name:
            to_print += f" name: {self.name}"
        return to_print

    def update_position(self, **kwargs):
        self.x = kwargs.get('x')
        self.y = kwargs.get('y')
        self.theta = kwargs.get('theta')
        self.name = kwargs.get('name')
        self.map = kwargs.get('map')

    def get_distance(self, other):
        return ((self.x - other.x) ** 2 + (self.y - other.y) ** 2) ** 0.5

    def slope(self, other):
        # m = (y2 - y1) / (x2- x1)
        try:
            return (other.y - self.y)/(other.x - self.x)
        except ZeroDivisionError:
            raise   # The slope is undefined, i.e. it is a vertical line

    def y_intercept(self, slope):
        # b = y - mx
        return self.y - slope * self.x

    def get_intersection_point(self, point1, point2):
        """Returns the intersection point between the lines:
            line 1: between point1 and point2
            line 2: perpendicular line between self and line 1
        """
        # Line 1:
        try:
            m1 = point1.slope(point2)
            b1 = point1.y_intercept(m1)

            if m1 == 0:
                # The perpendicular line of an horizontal line is a vertical line
                x = self.x
                y = point1.y
            else:
                # Perpendicular lines have a slope of -1
                # m1*m2 = -1
                m2 = -1 / m1

                # Line 2:  (y - self.y) = m2(x - self.x)
                b2 = self.y_intercept(m2)

                # line1 = line2
                # m1 * x + b1 = m2 * x + b2
                x = (b2 - b1) / (m1 - m2)
                y = m1 * x + b1

        except ZeroDivisionError:
            # Line 1 is a vertical line (its slope is undefined)
            x = point1.x

            # The perpendicular line to Line 1 is an horizontal line
            m2 = 0
            # Line 2: y = b
            b2 = self.y_intercept(m2)
            y = b2

        return Position(x, y)

    def get_closest_point(self, points):
        "Return the closest point (from the list of points) to this point"
        min_distance = np.inf
        closest_point = None
        for point in points:
            distance = self.get_distance(point)
            if distance < min_distance:
                min_distance = distance
                closest_point = point

        return closest_point


    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        return dict_repr

    @property
    def meta_model(self):
        return self.Meta.meta_model


class Checkpoint(Position):
    visited = fields.BooleanField(default=False)


