import uuid

import inflection
import pytz
from bson.codec_options import CodecOptions
from fmlib.api.zyre.messages import Document
from fmlib.models.environment import Position
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
    action_type = fields.CharField()
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

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr['_id'] = str(dict_repr.pop('_id'))
        return dict_repr

    def to_msg(self):
        msg = dict()
        msg['actionId'] = str(self.action_id)
        msg['actionType'] = inflection.camelize(self.action_type, False)
        msg['actionParameters'] = self.get_parameters_dict()
        return msg

    def get_parameters_dict(self):
        return dict()

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

    def get_parameters_dict(self):
        parameters = {"locations": []}
        for l in self.locations:
            parameters["locations"] = l.to_dict()
        return parameters


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


class DetectObject(Action):
    object_type = fields.CharField()

    def get_parameters_dict(self):
        return {"objectType": self.object_type}


class StartCharging(Action):
    until_percentage = fields.FloatField()
    duration = fields.FloatField()  # seconds

    def get_parameters_dict(self):
        return {"untilPercentage": self.until_percentage,
                "duration": self.duration}


class StopCharging(Action):
    pass


class Navigate(Action):
    start = fields.EmbeddedDocumentField(Position)
    goal = fields.EmbeddedDocumentField(Position)
    velocity = fields.FloatField(default=1.5)

    def get_parameters_dict(self):
        return {"start": self.start.to_dict(),
                "goal": self.goal.to_dict(),
                "velocity": self.velocity}


class StandStill(Action):
    duration = fields.FloatField()

    def get_parameters_dict(self):
        return {"duration": self.duration}


class FollowWall(Action):
    area = fields.CharField()
    start = fields.EmbeddedDocumentField(Position)
    checkpoints = fields.EmbeddedDocumentListField(Position)
    polygon = fields.EmbeddedDocumentListField(Position)
    velocity = fields.FloatField()

    objects = ActionManager()

    def get_parameters_dict(self):
        parameters = {"checkpoints": [],
                      "polygon": [],
                      "velocity": self.velocity}
        for c in self.checkpoints:
            parameters["checkpoints"].append(c.to_dict())
        for p in self.polygon:
            parameters["polygon"].append(p.to_dict())
        return parameters

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


class ActivateUVCLight(Action):
    pass


class DeactivateUVCLight(Action):
    pass


class ActionProgress(MongoModel, EmbeddedMongoModel):
    action_id = fields.UUIDField(primary_key=True)
    status = fields.IntegerField(default=ActionStatus.PLANNED)
    start_time = fields.DateTimeField()
    finish_time = fields.DateTimeField()

    objects = ActionProgressManager()

    class Meta:
        archive_collection = 'action_progress_archive'
        ignore_unknown_fields = True
        codec_options = CodecOptions(tz_aware=True, tzinfo=pytz.timezone('utc'))

    def archive(self):
        try:
            action = Action.get_action(self.action_id)
            action.archive()
        except DoesNotExist:
            pass
        with switch_collection(ActionProgress, ActionProgress.Meta.archive_collection):
            super().save()
        self.delete()

    @classmethod
    def get_action_progress(cls, action_id):
        try:
            return cls.objects.get_action_progress(action_id)
        except DoesNotExist:
            return cls.get_archived_action_progress(action_id)

    @classmethod
    def get_archived_action_progress(cls, action_id):
        with switch_collection(cls, cls.Meta.archive_collection):
            return cls.objects.get_action_progress(action_id)

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr['action_id'] = str(dict_repr.pop('_id'))
        if self.start_time:
            dict_repr['start_time'] = self.start_time.isoformat()
        if self.finish_time:
            dict_repr['finish_time'] = self.finish_time.isoformat()
        return dict_repr
