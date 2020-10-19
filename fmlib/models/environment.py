from pymodm import EmbeddedMongoModel, fields


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

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        return dict_repr

    @property
    def meta_model(self):
        return self.Meta.meta_model


class Checkpoint(Position):
    visited = fields.BooleanField(default=False)


