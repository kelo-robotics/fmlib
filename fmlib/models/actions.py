import uuid

from pymodm import EmbeddedMongoModel, fields, MongoModel
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from ropod.structs.status import ActionStatus


class ActionQuerySet(QuerySet):
    def get_action(self, action_id):
        if isinstance(action_id, str):
            action_id = uuid.UUID(action_id)
        return self.get({'_id': action_id})


ActionManager = Manager.from_queryset(ActionQuerySet)


class Duration(EmbeddedMongoModel):
    mean = fields.FloatField()
    variance = fields.FloatField()

    def update(self, mean, variance):
        self.mean = mean
        self.variance = variance


class Action(MongoModel, EmbeddedMongoModel):

    action_id = fields.UUIDField(primary_key=True)
    type = fields.CharField()
    duration = fields.EmbeddedDocumentField(Duration, blank=True)

    objects = ActionManager()

    class Meta:
        ignore_unknown_fields = True

    @classmethod
    def create_new(cls, **kwargs):
        if 'action_id' not in kwargs.keys():
            kwargs.update(action_id=uuid.uuid4())
        action = cls(**kwargs)
        action.save()
        return action

    def update_duration(self, mean, variance):
        if not self.duration:
            self.duration = Duration()
        self.duration.update(mean, variance)
        self.save()

    @classmethod
    def get_action(cls, action_id):
        return cls.objects.get_action(action_id)


class GoTo(Action):
    locations = fields.ListField()

    @property
    def source(self):
        return self.locations[0]

    @property
    def destination(self):
        return self.locations[-1]


class EnterElevator(Action):
    elevator_id = fields.IntegerField()


class ExitElevator(Action):
    pass


class WaitForElevator(Action):
    pass


class RequestElevator(Action):
    start_floor = fields.IntegerField()
    goal_floor = fields.IntegerField()


class RideElevator(Action):
    level = fields.IntegerField()


class Dock(Action):
    pass


class Undock(Action):
    pass


class ActionProgress(EmbeddedMongoModel):
    action = fields.ReferenceField(Action)
    status = fields.IntegerField(default=ActionStatus.PLANNED)
    start_time = fields.DateTimeField()
    finish_time = fields.DateTimeField()
