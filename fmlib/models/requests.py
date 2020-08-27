import logging

from pymodm import MongoModel, fields
from pymongo.errors import ServerSelectionTimeoutError
from ropod.structs.task import TaskPriority

from fmlib.models.users import User
from fmlib.utils.messages import Document
from datetime import datetime


class Request(MongoModel):

    request_id = fields.UUIDField(primary_key=True)
    user_id = fields.ReferenceField(User)


class TaskRequest(Request):

    priority = fields.IntegerField(default=TaskPriority.NORMAL)
    hard_constraints = fields.BooleanField(default=True)

    class Meta:
        archive_collection = 'task_request_archive'
        ignore_unknown_fields = True
        meta_model = "task-request"
        task_type = "Task"

    @property
    def task_type(self):
        return self.Meta.task_type

    def save(self):
        try:
            super().save(cascade=True)
        except ServerSelectionTimeoutError:
            logging.warning('Could not save models to MongoDB')

    @classmethod
    def from_payload(cls, payload, save=True):
        document = Document.from_payload(payload)
        document['_id'] = document.pop('request_id')
        document['user_id'] = User(user_id=document.pop('user_id'))
        request = cls.from_document(document)
        if save:
            request.save()
        return request

    @property
    def meta_model(self):
        return self.Meta.meta_model

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr["request_id"] = str(dict_repr.pop('_id'))
        return dict_repr


class TransportationRequest(TaskRequest):

    pickup_location = fields.CharField()
    pickup_location_level = fields.IntegerField()
    delivery_location = fields.CharField()
    delivery_location_level = fields.IntegerField()
    earliest_pickup_time = fields.DateTimeField()
    latest_pickup_time = fields.DateTimeField()
    load_type = fields.CharField()
    load_id = fields.CharField()

    class Meta:
        archive_collection = TaskRequest.Meta.archive_collection
        ignore_unknown_fields = TaskRequest.Meta.ignore_unknown_fields
        meta_model = TaskRequest.Meta.meta_model
        task_type = "TransportationTask"

    @property
    def start_location(self):
        return self.pickup_location

    @property
    def finish_location(self):
        return self.delivery_location

    @property
    def start_location_level(self):
        return self.pickup_location_level

    @property
    def finish_location_level(self):
        return self.delivery_location_level

    @property
    def earliest_start_time(self):
        return self.earliest_pickup_time

    def validate_request(self, path_planner):
        if self.pickup_location == self.delivery_location:
            raise InvalidRequestLocation("Pickup and delivery location are the same")
        elif self.latest_pickup_time < datetime.now():
            raise InvalidRequestTime("Latest start time of %s is in the past" % self.latest_pickup_time)
        elif not path_planner.is_valid_location(self.pickup_location, behaviour="docking"):
            raise InvalidRequestLocation("%s is not a valid pickup area." % self.pickup_location)
        elif not path_planner.is_valid_location(self.delivery_location, behaviour="undocking"):
            raise InvalidRequestLocation("%s is not a valid delivery area." % self.delivery_location)

    def to_dict(self):
        dict_repr = super().to_dict()
        dict_repr["earliest_pickup_time"] = self.earliest_pickup_time.isoformat()
        dict_repr["latest_pickup_time"] = self.latest_pickup_time.isoformat()
        return dict_repr


class NavigationRequest(TaskRequest):

    goal_location = fields.CharField()
    goal_location_level = fields.IntegerField()
    earliest_arrival_time = fields.DateTimeField()
    latest_arrival_time = fields.DateTimeField()
    wait_at_goal = fields.IntegerField()  # seconds

    class Meta:
        archive_collection = TaskRequest.Meta.archive_collection
        ignore_unknown_fields = TaskRequest.Meta.ignore_unknown_fields
        meta_model = TaskRequest.Meta.meta_model
        task_type = "NavigationTask"

    @property
    def start_location(self):
        return self.goal_location

    @property
    def finish_location(self):
        return self.goal_location

    @property
    def start_location_level(self):
        return self.goal_location_level

    @property
    def finish_location_level(self):
        return self.goal_location_level

    @property
    def earliest_start_time(self):
        return self.earliest_arrival_time

    def validate_request(self, path_planner):
        if self.latest_arrival_time < datetime.now():
            raise InvalidRequestTime("Latest start time of %s is in the past" % self.latest_pickup_time)
        elif not path_planner.is_valid_location(self.goal_location, behaviour="docking"):
            raise InvalidRequestLocation("%s is not a valid goal location." % self.pickup_location)

    def to_dict(self):
        dict_repr = super().to_dict()
        dict_repr["earliest_arrival_time"] = self.earliest_arrival_time.isoformat()
        dict_repr["latest_arrival_time"] = self.latest_arrival_time.isoformat()
        return dict_repr


class InvalidRequest(Exception):
    pass


class InvalidRequestLocation(InvalidRequest):
    pass


class InvalidRequestTime(InvalidRequest):
    pass

