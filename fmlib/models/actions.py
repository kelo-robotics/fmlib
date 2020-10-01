import uuid

from fmlib.models.environment import Position
from fmlib.utils.messages import Document
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

    @property
    def standard_dev(self):
        return round(self.variance ** 0.5, 3)

    def __str__(self):
        to_print = ""
        to_print += "N({}, {})".format(self.mean, self.standard_dev)
        return to_print

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        return cls.from_document(document)

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        return dict_repr

    def update(self, mean, variance):
        self.mean = round(mean, 3)
        self.variance = round(variance, 3)


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
        save_in_db = kwargs.pop("save_in_db", True)
        action = cls(**kwargs)
        if save_in_db:
            action.save()
        return action

    def update_duration(self, mean, variance, save_in_db=True):
        if not self.duration:
            self.duration = Duration()
        self.duration.update(mean, variance)
        if save_in_db:
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


class Navigation(Action):
    start = fields.EmbeddedDocumentField(Position)
    goal = fields.EmbeddedDocumentField(Position)
    velocity = fields.FloatField()


class StandStill(Action):
    pass


class WallFollowing(Action):
    checkpoints = fields.EmbeddedDocumentListField(Position)
    polygon = fields.EmbeddedDocumentListField(Position)
    velocity = fields.FloatField()


class SwitchOnLamp(Action):
    pass


class SwitchOffLamp(Action):
    pass


class ActionProgress(EmbeddedMongoModel):
    action = fields.ReferenceField(Action)
    status = fields.IntegerField(default=ActionStatus.PLANNED)
    start_time = fields.DateTimeField()
    finish_time = fields.DateTimeField()
