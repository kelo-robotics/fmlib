import logging
import sys
from datetime import datetime

from fmlib.models.users import User
from fmlib.utils.messages import Document
from pymodm import MongoModel, fields
from pymongo.errors import ServerSelectionTimeoutError
from ropod.structs.task import TaskPriority, DisinfectionDose

this_module = sys.modules[__name__]


class Request(MongoModel):

    request_id = fields.UUIDField(primary_key=True)
    user_id = fields.ReferenceField(User)


class TaskRequest(Request):

    priority = fields.IntegerField(default=TaskPriority.NORMAL)
    hard_constraints = fields.BooleanField(default=True)
    eligible_robots = fields.ListField(blank=True)

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

    def validate_request(self, path_planner):
        if self.latest_start_time < datetime.now():
            raise InvalidRequestTime("Latest start time of %s is in the past" % self.latest_arrival_time)
        elif not path_planner.is_valid_location(self.start_location):
            raise InvalidRequestLocation("%s is not a valid goal location." % self.start_location)
        elif not path_planner.is_valid_location(self.finish_location):
            raise InvalidRequestLocation("%s is not a valid goal location." % self.finish_location)


class TasksRequest(Request):
    requests = fields.EmbeddedDocumentListField(TaskRequest)

    class Meta:
        collection_name = "tasks_request"
        archive_collection = 'tasks_request_archive'
        ignore_unknown_fields = True
        meta_model = "tasks-request"

    @property
    def meta_model(self):
        return self.Meta.meta_model

    def save(self):
        try:
            super().save(cascade=True)
        except ServerSelectionTimeoutError:
            logging.warning('Could not save models to MongoDB')

    @classmethod
    def create_new(cls, **kwargs):
        request_ids = [request.request_id for request in kwargs.get("requests")]
        # All requests have the same request id
        if len(set(request_ids)) == 1:
            tasks_request = cls(request_id=request_ids[0], **kwargs)
            return tasks_request

    @classmethod
    def from_payload(cls, payload, save=True):
        document = Document.from_payload(payload)
        document['_id'] = document.pop('request_id')
        requests = list()
        for request in document['requests']:
            request_type = request.pop("_cls").split('.')[-1]
            request_cls = getattr(this_module, request_type)
            requests.append(request_cls.from_payload(request))
        document['requests'] = requests
        tasks_request = cls.from_document(document)
        if save:
            tasks_request.save()
        return tasks_request

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr["request_id"] = str(dict_repr.pop('_id'))
        requests = list()
        for request in self.requests:
            requests.append(request.to_dict())
        dict_repr["requests"] = requests
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

    @property
    def latest_start_time(self):
        return self.latest_pickup_time

    def validate_request(self, path_planner):
        super().validate_request(path_planner)
        if self.pickup_location == self.delivery_location:
            raise InvalidRequestLocation("Pickup and delivery location are the same")

    def to_dict(self):
        dict_repr = super().to_dict()
        dict_repr["earliest_pickup_time"] = self.earliest_pickup_time.isoformat()
        dict_repr["latest_pickup_time"] = self.latest_pickup_time.isoformat()
        return dict_repr


class NavigationRequest(TaskRequest):

    start_location = fields.CharField()
    start_location_level = fields.IntegerField()
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
    def finish_location(self):
        return self.goal_location

    @property
    def finish_location_level(self):
        return self.goal_location_level

    @property
    def earliest_start_time(self):
        return self.earliest_arrival_time

    @property
    def latest_start_time(self):
        return self.latest_arrival_time

    def to_dict(self):
        dict_repr = super().to_dict()
        dict_repr["earliest_arrival_time"] = self.earliest_arrival_time.isoformat()
        dict_repr["latest_arrival_time"] = self.latest_arrival_time.isoformat()
        return dict_repr


class GuidanceRequest(NavigationRequest):

    class Meta:
        archive_collection = TaskRequest.Meta.archive_collection
        ignore_unknown_fields = TaskRequest.Meta.ignore_unknown_fields
        meta_model = TaskRequest.Meta.meta_model
        task_type = "GuidanceTask"


class DisinfectionRequest(TaskRequest):
    area = fields.CharField()
    start_location = fields.CharField()
    finish_location = fields.CharField()
    earliest_start_time = fields.DateTimeField()
    latest_start_time = fields.DateTimeField()
    dose = fields.IntegerField(default=DisinfectionDose.NORMAL)

    class Meta:
        archive_collection = TaskRequest.Meta.archive_collection
        ignore_unknown_fields = TaskRequest.Meta.ignore_unknown_fields
        meta_model = TaskRequest.Meta.meta_model
        task_type = "DisinfectionTask"

    def get_velocity(self):
        """ Returns max velocity (m/s) based on the dose """
        if self.dose == DisinfectionDose.HIGH:
            velocity = 0.1
        elif self.dose == DisinfectionDose.NORMAL:
            velocity = 0.3
        elif self.dose == DisinfectionDose.LOW:
            velocity = 0.5
        else:
            print("Dose is invalid")
            raise ValueError(self.dose)
        return velocity

    def validate_request(self, path_planner):
        super().validate_request(path_planner)
        if not path_planner.is_valid_area(self.area):
            raise InvalidRequestArea("%s is not a valid area." % self.area)

    def complete_request(self, path_planner):
        self.start_location = path_planner.get_start_location(self.area)
        self.finish_location = self.start_location
        self.save()

    def to_dict(self):
        dict_repr = super().to_dict()
        dict_repr["earliest_start_time"] = self.earliest_start_time.isoformat()
        dict_repr["latest_start_time"] = self.latest_start_time.isoformat()
        return dict_repr


class InvalidRequest(Exception):
    pass


class InvalidRequestLocation(InvalidRequest):
    pass


class InvalidRequestArea(InvalidRequest):
    pass


class InvalidRequestTime(InvalidRequest):
    pass

