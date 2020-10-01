import logging
import sys
import uuid
from datetime import datetime

import dateutil.parser
from fmlib.models.users import User
from fmlib.utils.messages import Document
from pymodm import EmbeddedMongoModel, fields, MongoModel
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from pymongo.errors import ServerSelectionTimeoutError
from ropod.structs.task import TaskPriority, DisinfectionDose

this_module = sys.modules[__name__]


class RequestQuerySet(QuerySet):
    def get_request(self, request_id):
        if isinstance(request_id, str):
            request_id = uuid.UUID(request_id)
        return self.get({'_id': request_id})


RequestManager = Manager.from_queryset(RequestQuerySet)


class Request(MongoModel):
    request_id = fields.UUIDField(primary_key=True)
    user_id = fields.ReferenceField(User)

    objects = RequestManager()

    @classmethod
    def get_request(cls, request_id):
        if isinstance(request_id, str):
            request_id = uuid.UUID(request_id)
        return cls.objects.get_request(request_id)


class TaskRequest(EmbeddedMongoModel):
    task_id = fields.UUIDField(blank=True)
    parent_task_id = fields.UUIDField(blank=True)  # Indicates that this request was originated from an uncompleted task
    priority = fields.IntegerField(default=TaskPriority.NORMAL)
    hard_constraints = fields.BooleanField(default=True)
    eligible_robots = fields.ListField(blank=True)
    valid = fields.BooleanField()

    class Meta:
        ignore_unknown_fields = True
        task_type = "Task"

    @property
    def task_type(self):
        return self.Meta.task_type

    @classmethod
    def create_new(cls, **kwargs):
        request = cls(**kwargs)
        return request

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        request = cls.from_document(document)
        return request

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        return dict_repr

    def validate_request(self, path_planner, complete_request=True):
        if self.latest_start_time < datetime.now():
            raise InvalidRequestTime("Latest start time of %s is in the past" % self.latest_start_time)
        elif not path_planner.is_valid_location(self.start_location):
            raise InvalidRequestLocation("%s is not a valid goal location." % self.start_location)
        elif not path_planner.is_valid_location(self.finish_location):
            raise InvalidRequestLocation("%s is not a valid goal location." % self.finish_location)


class TaskRequests(Request):
    requests = fields.EmbeddedDocumentListField(TaskRequest)

    objects = RequestManager()

    class Meta:
        collection_name = "task_request"
        archive_collection = 'task_request_archive'
        ignore_unknown_fields = True
        meta_model = "task-request"

    def update_task_id(self, request, task_id):
        request.task_id = task_id
        self.save()

    def mark_as_invalid(self, request):
        request.valid = False
        self.save()

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
        if "request_id" not in kwargs.keys():
            kwargs.update(request_id=uuid.uuid4())
        request = cls(**kwargs)
        return request

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
        dict_repr.pop('_cls')
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

    def validate_request(self, path_planner, complete_request=True):
        super().validate_request(path_planner, complete_request)
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
        task_type = "GuidanceTask"


class DisinfectionRequest(TaskRequest):
    area = fields.CharField()
    start_location = fields.CharField()
    finish_location = fields.CharField()
    earliest_start_time = fields.DateTimeField()
    latest_start_time = fields.DateTimeField()
    dose = fields.IntegerField(default=DisinfectionDose.NORMAL)

    class Meta:
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

    def validate_request(self, path_planner, complete_request=True):
        if not path_planner.is_valid_area(self.area):
            raise InvalidRequestArea("%s is not a valid area." % self.area)
        if complete_request:
            self.complete_request(path_planner)
        super().validate_request(path_planner, complete_request)

    def complete_request(self, path_planner):
        self.start_location = path_planner.get_start_location(self.area)
        self.finish_location = path_planner.get_finish_location(self.area)

    def to_dict(self):
        dict_repr = super().to_dict()
        dict_repr["earliest_start_time"] = self.earliest_start_time.isoformat()
        dict_repr["latest_start_time"] = self.latest_start_time.isoformat()
        return dict_repr

    def from_task(self, task, **kwargs):
        if "earliest_start_time" in kwargs:
            kwargs["earliest_start_time"] = dateutil.parser.parse(kwargs.pop("earliest_start_time"))

        if "latest_start_time" in kwargs:
            kwargs["latest_start_time"] = dateutil.parser.parse(kwargs.pop("latest_start_time"))

        for attr in self.__dict__['_data'].__dict__['_members']:
            if attr not in kwargs:
                kwargs[attr] = getattr(self, attr)
        kwargs.update(parent_task_id=task.task_id)
        request = self.create_new(**kwargs)
        return request


class InvalidRequest(Exception):
    pass


class InvalidRequestLocation(InvalidRequest):
    pass


class InvalidRequestArea(InvalidRequest):
    pass


class InvalidRequestTime(InvalidRequest):
    pass

