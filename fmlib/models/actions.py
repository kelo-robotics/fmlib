import uuid

from fmlib.models.environment import Position
from fmlib.utils.messages import Document
from pymodm import EmbeddedMongoModel, fields, MongoModel
from pymodm.context_managers import switch_collection
from pymodm.errors import DoesNotExist
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from ropod.structs.status import ActionStatus


class ActionQuerySet(QuerySet):

    def get_action(self, action_id):
        if isinstance(action_id, str):
            action_id = uuid.UUID(action_id)
        return self.get({'_id': action_id})

    def by_action_type(self, type):
        return self.raw({"type": type})


ActionManager = Manager.from_queryset(ActionQuerySet)

class ActionProgressQuerySet(QuerySet):

    def get_action_progress(self, action_id):
        if isinstance(action_id, str):
            action_id = uuid.UUID(action_id)
        return self.get({'_id': action_id})

ActionProgressManager = Manager.from_queryset(ActionProgressQuerySet)


class EstimatedDuration(EmbeddedMongoModel):
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
    estimated_duration = fields.EmbeddedDocumentField(EstimatedDuration, blank=True)
    pre_task_action = fields.BooleanField(default=False)

    objects = ActionManager()

    class Meta:
        archive_collection = 'action_archive'
        ignore_unknown_fields = True

    @property
    def progress(self):
        return self.get_action_progress(self.action_id)

    @classmethod
    def create_new(cls, **kwargs):
        if 'action_id' not in kwargs.keys():
            kwargs.update(action_id=uuid.uuid4())
        save_in_db = kwargs.pop("save_in_db", True)
        action = cls(**kwargs)
        if save_in_db:
            action.save()
        return action

    def archive(self):
        with switch_collection(self, self.Meta.archive_collection):
            super().save()
        self.delete()

    def update_duration(self, mean, variance, save_in_db=True):
        if not self.estimated_duration:
            self.estimated_duration = EstimatedDuration()
        self.estimated_duration.update(mean, variance)
        if save_in_db:
            self.save()

    def from_action(self):
        kwargs = dict()
        for attr in self.__dict__['_data'].__dict__['_members']:
            kwargs[attr] = getattr(self, attr)
        kwargs.update(action_id=uuid.uuid4())
        action = self.create_new(**kwargs)
        return action

    @classmethod
    def get_action(cls, action_id):
        try:
            return cls.objects.get_action(action_id)
        except DoesNotExist:
            return cls.get_archived_action(action_id)

    @classmethod
    def get_archived_action(cls, action_id):
        with switch_collection(cls, cls.Meta.archive_collection):
            return cls.objects.get_action(action_id)

    @classmethod
    def get_actions(cls, type=None):
        if type:
            return [action for action in cls.objects.by_action_type(type)]
        return [action for action in cls.objects.all()]

    @classmethod
    def get_archived_actions(cls, type=None):
        with switch_collection(cls, cls.Meta.archive_collection):
            if type:
                return [action for action in cls.objects.by_action_type(type)]
            return [action for action in cls.objects.all()]

    @staticmethod
    def get_action_progress(action_id):
        return ActionProgress.objects.get_action_progress(action_id)

    @staticmethod
    def get_archived_action_progress(action_id):
        with switch_collection(ActionProgress, ActionProgress.Meta.archive_collection):
            return ActionProgress.objects.get_action_progress(action_id)

    def get_action_duration(self):
        try:
            action_progress = self.get_archived_action_progress(self.action_id)

            if action_progress.finish_time and action_progress.start_time:
                return (action_progress.finish_time - action_progress.start_time).total_seconds()
            # The duration is not known yet (the action has not been executed)
            return None

        except DoesNotExist:
            return None


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
    velocity = fields.FloatField(default=1.5)


class Standstill(Action):
    duration = fields.FloatField()


class WallFollowing(Action):
    area = fields.CharField()
    start = fields.EmbeddedDocumentField(Position)
    checkpoints = fields.EmbeddedDocumentListField(Position)
    polygon = fields.EmbeddedDocumentListField(Position)
    velocity = fields.FloatField()

    objects = ActionManager()

    @classmethod
    def get_actions(cls, area=None, velocity=None):
        actions = super().get_actions(type="WALL_FOLLOWING")
        if area:
            actions = [action for action in actions if action.area == area]
        if velocity:
            actions = [action for action in actions if action.velocity == velocity]
        return actions

    @classmethod
    def get_archived_actions(cls, area=None, velocity=None):
        actions = super().get_archived_actions(type="WALL_FOLLOWING")
        if area:
            actions = [action for action in actions if action.area == area]
        if velocity:
            actions = [action for action in actions if action.velocity == velocity]
        return actions


class LampControl(Action):
    switch_on = fields.BooleanField()


class ActionProgress(MongoModel, EmbeddedMongoModel):
    action_id = fields.UUIDField(primary_key=True)
    status = fields.IntegerField(default=ActionStatus.PLANNED)
    start_time = fields.DateTimeField()
    finish_time = fields.DateTimeField()

    objects = ActionProgressManager()

    class Meta:
        archive_collection = 'action_progress_archive'
        ignore_unknown_fields = True

    def archive(self):
        try:
            action = Action.get_action(self.action_id)
            action.archive()
        except DoesNotExist:
            pass
        with switch_collection(ActionProgress, ActionProgress.Meta.archive_collection):
            super().save()
        self.delete()
