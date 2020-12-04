import dateutil.parser
from fmlib.utils.messages import Document
from pymodm import EmbeddedMongoModel, fields


class Timepoint(EmbeddedMongoModel):
    utc_time = fields.DateTimeField()
    timezone_offset = fields.FloatField()

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr["utc_time"] = self.utc_time.isoformat()
        return dict_repr

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        document['utc_time'] = dateutil.parser.parse(document.pop("utc_time"))
        timepoint = cls.from_document(document)
        return timepoint


class Position(EmbeddedMongoModel):

    x = fields.FloatField()
    y = fields.FloatField()
    theta = fields.FloatField(default=0)

    class Meta:
        ignore_unknown_fields = True
        meta_model = "position"

    def __eq__(self, other):
        if not isinstance(other, Position):
            return False
        return (self.x == other.x
                and self.y == other.y
                and self.theta == other.theta)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return "[{}, {}, {}]".format(self.x, self.y, self.theta)

    def update_2d_pose(self, x, y, theta):
        self.x = x
        self.y = y
        self.theta = theta

    def get_distance(self, other):
        return ((self.x - other.x) ** 2 + (self.y - other.y) ** 2) ** 0.5

    def slope(self, other):
        # m = (y2 - y1) / (x2- x1)
        return (other.y - self.y)/(other.x - self.x)

    def y_intercept(self, slope):
        # b = y - mx
        return self.y - slope * self.x

    def get_intersection_point(self, point1, point2):
        """Returns the intersection point between the lines:
            line 1: between point1 and point2
            line 2: perpendicular line between self and line 1
        """
        # Line 1:
        m1 = point1.slope(point2)
        b1 = point1.y_intercept(m1)

        # Perpendicular lines have a slope of -1
        # m1*m2 = -1
        m2 = -1 /m1

        # Line 2:  (y - self.y) = m2(x - self.x)
        b2 = self.y_intercept(m2)

        # line1 = line2
        # m1 * x + b1 = m2 * x + b2
        x = (b2 - b1) / (m1 - m2)
        y = m1 * x + b1

        return Position(x, y)

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        return dict_repr

    @property
    def meta_model(self):
        return self.Meta.meta_model


class Checkpoint(Position):
    visited = fields.BooleanField(default=False)


