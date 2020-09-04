from pymodm import EmbeddedMongoModel, fields


class Position(EmbeddedMongoModel):

    x = fields.FloatField()
    y = fields.FloatField()
    theta = fields.FloatField(default=0)

    class Meta:
        ignore_unknown_fields = True

    def update_2d_pose(self, x, y, theta):
        self.x = x
        self.y = y
        self.theta = theta

    def get_distance(self, other):
        return ((self.x - other.x) ** 2 + (self.y - other.y) ** 2) ** 0.5
