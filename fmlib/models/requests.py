import logging
import sys
import uuid

import pytz
from fmlib.models.environment import Timepoint
from fmlib.models.users import User
from fmlib.utils.messages import Document
from pymodm import EmbeddedMongoModel, fields, MongoModel
from pymodm.context_managers import switch_collection
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from pymongo.errors import ServerSelectionTimeoutError
from ropod.structs.task import TaskPriority, DisinfectionDose
from ropod.utils.timestamp import TimeStamp

this_module = sys.modules[__name__]


class RequestQuerySet(QuerySet):
    def get_request(self, request_id):
        if isinstance(request_id, str):
            request_id = uuid.UUID(request_id)
        return self.get({'_id': request_id})


RequestManager = Manager.from_queryset(RequestQuerySet)


class TaskRequestQuerySet(QuerySet):

    def by_event_uid(self, event_uid):
        if isinstance(event_uid, str):
            try:
                event_uid = uuid.UUID(event_uid)
            except ValueError as e:
                raise e

        return self.raw({'event_uid': event_uid})

TaskRequestManager = Manager.from_queryset(TaskRequestQuerySet)


class Request(MongoModel):
    request_id = fields.UUIDField(primary_key=True)
    user_id = fields.ReferenceField(User)

    objects = RequestManager()

    class Meta:
        update_collection = "request_update"
        archive_collection = 'request_archive'
        ignore_unknown_fields = True

    @classmethod
    def get_request(cls, request_id):
        if isinstance(request_id, str):
            request_id = uuid.UUID(request_id)
        return cls.objects.get_request(request_id)


class TaskRequest(MongoModel):
    task_id = fields.UUIDField(blank=True)
    parent_task_id = fields.UUIDField(blank=True)  # Indicates that this request was originated from an uncompleted task
    priority = fields.IntegerField(default=TaskPriority.NORMAL)
    hard_constraints = fields.BooleanField(default=True)
    eligible_robots = fields.ListField(blank=True)
    valid = fields.BooleanField()
    event_uid = fields.UUIDField(blank=True)  # Indicates that this request was originated from an event id
    rrule = fields.DictField(blank=True) # recurrent rule to create a recurrent event
    exdates = fields.ListField(blank=True) # list of dates to exclude from the rrule
    summary = fields.CharField(blank=True) # Description of the task (to be shown in the calendar)

    objects = TaskRequestManager()

    class Meta:
        archive_collection = "task_request_archive"
        ignore_unknown_fields = True
        task_type = "Task"

    def archive(self):
        with switch_collection(self, TaskRequest.Meta.archive_collection):
            super().save()
        self.delete()

    @property
    def task_type(self):
        return self.Meta.task_type

    def update_task_id(self, task_id):
        self.task_id = task_id
        self.save()

    @classmethod
    def create_new(cls, **kwargs):
        try:
            exdates = kwargs.pop("exdates")
            parsed_exdates = list()
            for d in exdates:
                parsed_exdates.append(d.isoformat())
            kwargs.update(exdates=parsed_exdates)
        except KeyError:
            pass
        request = cls(**kwargs)
        request.save()
        return request

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        request = cls.from_document(document)
        request.save()
        return request

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        return dict_repr

    def validate_request(self, path_planner, complete_request=True):
        if self.latest_start_time.utc_time < TimeStamp().to_datetime():
            raise InvalidRequestTime("Latest start time of %s is in the past" % self.latest_start_time)
        elif not path_planner.is_valid_location(self.start_location):
            raise InvalidRequestLocation("%s is not a valid goal location." % self.start_location)
        elif not path_planner.is_valid_location(self.finish_location):
            raise InvalidRequestLocation("%s is not a valid goal location." % self.finish_location)

    @staticmethod
    def map_args(**kwargs):
        return kwargs

    @classmethod
    def from_dict(cls, **kwargs):
        return kwargs

    def from_task(self, task, **kwargs):
        kwargs = self.from_dict(**kwargs)

        for attr in self.__dict__['_data'].__dict__['_members']:
            if attr not in kwargs:
                kwargs[attr] = getattr(self, attr)
        kwargs.update(parent_task_id=task.task_id)
        request = self.create_new(**kwargs)
        return request

    @classmethod
    def get_task_requests(cls):
        return [task_request for task_request in cls.objects.all()]

    @classmethod
    def get_task_requests_by_event(cls, event_uid):
        return [task_request for task_request in cls.objects.by_event_uid(event_uid)]

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_id')
        return dict_repr


class TaskRequests(Request):
    requests = fields.ListField(fields.ReferenceField(TaskRequest))

    objects = RequestManager()

    class Meta:
        ignore_unknown_fields = True
        meta_model = "task-request"

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
        for r in request.requests:
            r.save()
        request.save()
        return request

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        document['_id'] = document.pop('request_id')
        requests = list()
        for request in document['requests']:
            request_type = request.pop("_cls").split('.')[-1]
            request_cls = getattr(this_module, request_type)
            requests.append(request_cls.from_payload(request))
        document['requests'] = requests
        task_requests = cls.from_document(document)
        task_requests.save()
        return task_requests

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr["request_id"] = str(dict_repr.pop('_id'))
        requests = list()
        for request in self.requests:
            requests.append(request.to_dict())
        dict_repr["requests"] = requests
        return dict_repr

    @classmethod
    def get_requests(cls):
        return [request for request in cls.objects.all()]

    def get_task_requests_by_event_uid(self, event_uid):
        return TaskRequest.get_task_requests_by_event(event_uid)

class TransportationRequest(TaskRequest):

    pickup_location = fields.CharField()
    pickup_location_level = fields.IntegerField()
    delivery_location = fields.CharField()
    delivery_location_level = fields.IntegerField()
    earliest_pickup_time = fields.EmbeddedDocumentField(Timepoint)
    latest_pickup_time = fields.EmbeddedDocumentField(Timepoint)
    load_type = fields.CharField()
    load_id = fields.CharField()

    objects = TaskRequestManager()

    class Meta:
        task_type = "TransportationTask"

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        document["earliest_pickup_time"] = Timepoint.from_payload(document.pop("earliest_pickup_time"))
        document["latest_pickup_time"] = Timepoint.from_payload(document.pop("latest_pickup_time"))
        request = cls.from_document(document)
        request.save()
        return request

    @classmethod
    def from_recurring_event(cls, event):
        timezone_offset = event.start.utcoffset().total_seconds()/60
        earliest_pickup_time = Timepoint(event.start.astimezone(pytz.utc), timezone_offset)
        latest_pickup_time = Timepoint(event.start.astimezone(pytz.utc), timezone_offset)
        latest_pickup_time.postpone(event.start_delta)

        return cls.create_new(event_uid=event.uid,
                              summary = event.summary,
                              pickup_location=event.pickup_location,
                              delivery_location=event.delivery_location,
                              earliest_pickup_time=earliest_pickup_time,
                              latest_pickup_time=latest_pickup_time,
                              load_type=event.load_type,
                              load_id=event.load_id)

    @property
    def start_location(self):
        return self.pickup_location

    @property
    def finish_location(self):
        return self.delivery_location

    @finish_location.setter
    def finish_location(self, location):
        self.delivery_location = location

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
        dict_repr["earliest_pickup_time"] = self.earliest_pickup_time.to_dict()
        dict_repr["latest_pickup_time"] = self.latest_pickup_time.to_dict()
        return dict_repr

    @staticmethod
    def map_args(**kwargs):
        map_keys = {'start_location': 'pickup_location',
                    'earliest_start_time': 'earliest_pickup_time',
                    'latest_start_time': 'latest_pickup_time'}
        kwargs = {map_keys[key]: value for key, value in kwargs.items()}
        return kwargs

    @classmethod
    def from_dict(cls, **kwargs):
        if "earliest_pickup_time" in kwargs:
            kwargs["earliest_pickup_time"] = Timepoint.from_payload(kwargs.pop("earliest_pickup_time"))

        if "latest_pickup_time" in kwargs:
            kwargs["latest_pickup_time"] = Timepoint.from_payload(kwargs.pop("latest_pickup_time"))
        return kwargs


class NavigationRequest(TaskRequest):

    start_location = fields.CharField()
    start_location_level = fields.IntegerField()
    goal_location = fields.CharField()
    goal_location_level = fields.IntegerField()
    earliest_arrival_time = fields.EmbeddedDocumentField(Timepoint)
    latest_arrival_time = fields.EmbeddedDocumentField(Timepoint)
    wait_at_goal = fields.IntegerField(default=0)  # seconds

    objects = TaskRequestManager()

    class Meta:
        task_type = "NavigationTask"

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        document["earliest_arrival_time"] = Timepoint.from_payload(document.pop("earliest_arrival_time"))
        document["latest_arrival_time"] = Timepoint.from_payload(document.pop("latest_arrival_time"))
        request = cls.from_document(document)
        request.save()
        return request

    @classmethod
    def from_recurring_event(cls, event):
        timezone_offset = event.start.utcoffset().total_seconds()/60
        earliest_arrival_time = Timepoint(event.start.astimezone(pytz.utc), timezone_offset)
        latest_arrival_time = Timepoint(event.start.astimezone(pytz.utc), timezone_offset)
        latest_arrival_time.postpone(event.start_delta)

        return cls.create_new(event_uid=event.uid,
                              summary=event.summary,
                              start_location=event.start_location,
                              goal_location=event.goal_location,
                              earliest_arrival_time=earliest_arrival_time,
                              latest_arrival_time=latest_arrival_time,
                              wait_at_goal=event.wait_at_goal)

    @property
    def finish_location(self):
        return self.goal_location

    @finish_location.setter
    def finish_location(self, location):
        self.goal_location = location

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
        dict_repr["earliest_arrival_time"] = self.earliest_arrival_time.to_dict()
        dict_repr["latest_arrival_time"] = self.latest_arrival_time.to_dict()
        return dict_repr

    @staticmethod
    def map_args(**kwargs):
        map_keys = {'start_location': 'start_location',
                    'earliest_start_time': 'earliest_arrival_time',
                    'latest_start_time': 'latest_arrival_time'}
        kwargs = {map_keys[key]: value for key, value in kwargs.items()}
        return kwargs

    @classmethod
    def from_dict(cls, **kwargs):
        if "earliest_arrival_time" in kwargs:
            kwargs["earliest_arrival_time"] = Timepoint.from_payload(kwargs.pop("earliest_arrival_time"))

        if "latest_arrival_time" in kwargs:
            kwargs["latest_arrival_time"] = Timepoint.from_payload(kwargs.pop("latest_arrival_time"))
        return kwargs

class DefaultNavigationRequest(NavigationRequest):
    """ Return to default waiting location
    """

    objects = TaskRequestManager()

    class Meta:
        task_type = "DefaultNavigationTask"


class GuidanceRequest(NavigationRequest):

    objects = TaskRequestManager()

    class Meta:
        task_type = "GuidanceTask"


class DisinfectionRequest(TaskRequest):
    area = fields.CharField()
    start_location = fields.CharField()
    finish_location = fields.CharField()
    earliest_start_time = fields.EmbeddedDocumentField(Timepoint)
    latest_start_time = fields.EmbeddedDocumentField(Timepoint)
    dose = fields.IntegerField(default=DisinfectionDose.NORMAL)

    objects = TaskRequestManager()

    class Meta:
        task_type = "DisinfectionTask"

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        document["earliest_start_time"] = Timepoint.from_payload(document.pop("earliest_start_time"))
        document["latest_start_time"] = Timepoint.from_payload(document.pop("latest_start_time"))
        request = cls.from_document(document)
        request.save()
        return request

    @classmethod
    def from_recurring_event(cls, event):
        timezone_offset = event.start.utcoffset().total_seconds()/60
        earliest_start_time = Timepoint(event.start.astimezone(pytz.utc), timezone_offset)
        latest_start_time = Timepoint(event.start.astimezone(pytz.utc), timezone_offset)
        latest_start_time.postpone(event.start_delta)

        return cls.create_new(event_uid=event.uid,
                              summary=event.summary,
                              area=event.area,
                              earliest_start_time=earliest_start_time,
                              latest_start_time=latest_start_time)

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
        if self.dose not in [DisinfectionDose.HIGH, DisinfectionDose.NORMAL, DisinfectionDose.LOW]:
            raise InvalidRequest("%s is not a valid disinfection dose " % self.dose)
        if not path_planner.is_valid_area(self.area):
            raise InvalidRequestArea("%s is not a valid area." % self.area)
        if complete_request:
            self.complete_request(path_planner)
        super().validate_request(path_planner, complete_request)

    def complete_request(self, path_planner):
        self.start_location = path_planner.get_start_location(self.area)
        self.finish_location = path_planner.get_finish_location(self.area)
        self.save()

    def to_dict(self):
        dict_repr = super().to_dict()
        dict_repr["earliest_start_time"] = self.earliest_start_time.to_dict()
        dict_repr["latest_start_time"] = self.latest_start_time.to_dict()
        return dict_repr

    @classmethod
    def from_dict(cls, **kwargs):
        if "earliest_start_time" in kwargs:
            kwargs["earliest_start_time"] = Timepoint.from_payload(kwargs.pop("earliest_start_time"))

        if "latest_start_time" in kwargs:
            kwargs["latest_start_time"] = Timepoint.from_payload(kwargs.pop("latest_start_time"))
        return kwargs


class InvalidRequest(Exception):
    pass


class InvalidRequestLocation(InvalidRequest):
    pass


class InvalidRequestArea(InvalidRequest):
    pass


class InvalidRequestTime(InvalidRequest):
    pass

