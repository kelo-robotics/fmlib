import inspect
import logging
import sys
import uuid
from datetime import timedelta

import dateutil.parser
import pytz
from bson.codec_options import CodecOptions
from fmlib.models.environment import Position
from fmlib.models.environment import Timepoint
from fmlib.models.event import Event
from fmlib.models.robot import Robot
from fmlib.models.users import User
from fmlib.utils.messages import Document
from pymodm import EmbeddedMongoModel, fields, MongoModel
from pymodm.context_managers import switch_collection
from pymodm.errors import ValidationError
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from pymongo.errors import ServerSelectionTimeoutError
from ropod.structs.task import TaskPriority, DisinfectionDose
from ropod.utils.timestamp import TimeStamp

this_module = sys.modules[__name__]


class RequestQuerySet(QuerySet):

    def validate_model(self, request):
        try:
            request.full_clean()
            return request
        except ValidationError:
            print(f"Request {request.request_id} has a deprecated format")
            request.deprecate()
            raise

    def get_request(self, request_id):
        if isinstance(request_id, str):
            request_id = uuid.UUID(request_id)
        request = self.get({'_id': request_id})
        return self.validate_model(request)


RequestManager = Manager.from_queryset(RequestQuerySet)


class Request(MongoModel):
    request_id = fields.UUIDField(primary_key=True)
    user_id = fields.ReferenceField(User)

    objects = RequestManager()

    class Meta:
        archive_collection = 'request_archive'
        ignore_unknown_fields = True

    @classmethod
    def get_request(cls, request_id):
        return cls.objects.get_request(request_id)

    @classmethod
    def get_archived_request(cls, request_id):
        with switch_collection(cls, cls.Meta.archive_collection):
            return cls.get_request(request_id)

    @classmethod
    def get_unregognized_field(cls, document):
        try:
            request = cls.from_document(document)
            return None
        except ValueError as e:
            if "Unrecognized field name" in str(e):
                unrecognized_field = str(e).split("'")[-2]
                print("unrecognized field: ", unrecognized_field)
                return unrecognized_field

    @classmethod
    def get_all_requests(cls):
        # Get requests as dict instances instead of as Model instances
        requests = cls.objects.all().values()
        valid_requests = list()
        deprecated_requests = list()
        for r in requests:
            deprecate = False
            while cls.get_unregognized_field(r):
                unrecognized_field = cls.get_unregognized_field(r)
                r.pop(unrecognized_field)
                deprecate = True

            request = cls.from_document(r)

            if deprecate:
                request.deprecate()
                deprecate = False

            try:
                cls.objects.validate_model(request)
                valid_requests.append(request)
            except ValidationError:
                deprecated_requests.append(request)

        return valid_requests

    @classmethod
    def get_all_archived_requests(cls):
        with switch_collection(cls, cls.Meta.archive_collection):
            return cls.get_all_requests()


class RepetitionPattern(EmbeddedMongoModel):
    until = fields.DateTimeField()
    count = fields.IntegerField()
    open_end = fields.BooleanField()

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        if dict_repr.get("until"):
            dict_repr["until"] = self.until.isoformat()
        return dict_repr

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        if document.get("until"):
            until = dateutil.parser.parse(document.pop("until"))
            until = until.replace(microsecond=0)
            document['until'] = until
        repetition_pattern = cls.from_document(document)
        return repetition_pattern


class TaskRequest(Request):
    request_id = fields.UUIDField(primary_key=True)
    task_ids = fields.ListField()
    priority = fields.IntegerField(default=TaskPriority.NORMAL)
    hard_constraints = fields.BooleanField(default=True)
    eligible_robots = fields.ListField(blank=True)
    map = fields.CharField()
    valid = fields.BooleanField()
    repetition_pattern = fields.EmbeddedDocumentField(RepetitionPattern)
    event = fields.ReferenceField(Event)

    objects = RequestManager()

    class Meta:
        collection_name = "task_request"
        archive_collection = "task_request_archive"
        ignore_unknown_fields = True
        codec_options = CodecOptions(tz_aware=True, tzinfo=pytz.timezone('utc'))
        task_type = "Task"

    def save(self):
        try:
            super().save(cascade=True)
        except ServerSelectionTimeoutError:
            logging.warning('Could not save models to MongoDB')

    def archive(self):
        with switch_collection(self, TaskRequest.Meta.archive_collection):
            self.save()
        self.delete()

    @property
    def task_type(self):
        return self.Meta.task_type

    def update_task_ids(self, task_id):
        if not self.task_ids:
            self.task_ids = list()
        self.task_ids.append(task_id)
        self.save()

    def mark_as_invalid(self):
        self.valid = False
        self.save()

    @classmethod
    def create_new(cls, **kwargs):
        if "request_id" not in kwargs.keys():
            kwargs.update(request_id=uuid.uuid4())
        event = kwargs.get("event")
        if isinstance(event, dict):
            event = Event.create_new(**kwargs.pop("event"))
            kwargs.update(event=event.uid)
        request = cls(**kwargs)
        request.save()
        return request

    def repeat(self, estimated_finish_time):
        if self.repetition_pattern.until:
            current_time = TimeStamp().to_datetime()
            if current_time < self.repetition_pattern.until and estimated_finish_time < self.repetition_pattern.until:
                return True
        if self.repetition_pattern.count and len(self.task_ids) < self.repetition_pattern.count:
            return True
        if self.repetition_pattern.open_end:
            return True

        return False

    @classmethod
    def to_document(cls, payload):
        document = Document.from_payload(payload)
        document['_id'] = document.pop('request_id')
        try:
            event = document.pop("event")
            if event:
                event.update(task_type=cls.Meta.task_type)
                document["event"] = Event.from_payload(event)
        except KeyError:
            pass
        try:
            repetition_pattern = document.pop("repetition_pattern")
            if repetition_pattern:
                document['repetition_pattern'] = RepetitionPattern.from_payload(repetition_pattern)
        except KeyError:
            pass
        return document

    def is_recurrent(self):
        if self.event:
            return True
        return False

    def is_repetitive(self):
        if self.repetition_pattern:
            return True
        return False

    def validate_request(self, path_planner, complete_request=True):
        if self.latest_start_time.utc_time < TimeStamp().to_datetime():
            raise InvalidRequestTime("Latest start time %s is in the past" % self.latest_start_time)
        elif self.latest_start_time < self.earliest_start_time:
            raise InvalidRequestTime("Latest start time %s is earlier than the earliest start time %s",
                                     self.latest_start_time, self.earliest_start_time)
        elif not path_planner.is_valid_location(self.map, self.start_location):
            raise InvalidRequestLocation("%s is not a valid goal location." % self.start_location)
        elif not path_planner.is_valid_location(self.map, self.finish_location):
            raise InvalidRequestLocation("%s is not a valid goal location." % self.finish_location)
        elif self.repetition_pattern and self.repetition_pattern.until and\
                self.repetition_pattern.until < TimeStamp().to_datetime():
            raise InvalidRequestTime("Repetition until time %s is in the past" % self.repetition_pattern.until)

    @staticmethod
    def map_args(**kwargs):
        return kwargs

    @classmethod
    def parse_dict(cls, **kwargs):
        return Event.parse_dict(**kwargs)

    @classmethod
    def get_task_requests(cls):
        return [task_request for task_request in cls.get_all_requests()]

    @classmethod
    def get_archived_task_requests(cls):
        return [task_request for task_request in cls.get_all_archived_requests()]

    @classmethod
    def get_task_requests_by_event(cls, event_uid):
        if isinstance(event_uid, str):
            event_uid = uuid.UUID(event_uid)
        task_requests = list()
        for task_request in cls.get_task_requests():
            if task_request.event and task_request.event.uid == event_uid:
                task_requests.append(task_request)
        return task_requests

    @classmethod
    def get_archived_task_requests_by_event(cls, event_uid):
        if isinstance(event_uid, str):
            event_uid = uuid.UUID(event_uid)
        task_requests = list()
        for task_request in cls.get_archived_task_requests():
            if task_request.event and task_request.event.uid == event_uid:
                task_requests.append(task_request)
        return task_requests

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr["request_id"] = str(dict_repr.pop('_id'))
        if self.repetition_pattern:
            dict_repr["repetition_pattern"] = self.repetition_pattern.to_dict()
        if self.event:
            dict_repr["event"] = self.event.to_dict()
        if self.task_ids:
            parsed_task_ids = list()
            for task_id in self.task_ids:
                parsed_task_ids.append(str(task_id))
            dict_repr["task_ids"] = parsed_task_ids
        return dict_repr

    def deprecate(self):
        """ The request has a deprecated format. Remove the request from the "task_request" collection and store it in the
        "task_request_archive" collection. Only the fields that remain valid are stored in the archive_collection.
        """

        for field in self._mongometa.get_fields():
            try:
                field_value = field.value_from_object(self)
                field_empty = field.is_undefined(self)
                if field_empty and field.required:
                    setattr(self, field.attname, None)
                elif not field_empty:
                    field.validate(field_value)
            except Exception as exc:
                if field.attname == "request_id":
                    self.request_id = uuid.uuid4()
                else:
                    delattr(self, field.attname)
        self.archive()

    @staticmethod
    def get_request_cls_from_icalendar_event(event):
        for name, request_cls in inspect.getmembers(this_module, inspect.isclass):
            if 'Meta' in request_cls.__dict__ and \
                    hasattr(request_cls.__dict__['Meta'], 'task_type') and \
                    event.task_type == request_cls.__dict__['Meta'].task_type:
                return request_cls

    def get_common_attrs(self):
        ignore_attrs = ["task_ids", "request_id"]
        kwargs = dict()
        for attr in self.__dict__['_data'].__dict__['_members']:
            if attr not in ignore_attrs:
                kwargs[attr] = getattr(self, attr)
        return  kwargs


class TransportationRequest(TaskRequest):

    pickup_location = fields.EmbeddedDocumentField(Position)
    pickup_location_level = fields.IntegerField()
    delivery_location = fields.EmbeddedDocumentField(Position)
    delivery_location_level = fields.IntegerField()
    earliest_pickup_time = fields.EmbeddedDocumentField(Timepoint)
    latest_pickup_time = fields.EmbeddedDocumentField(Timepoint)
    load_type = fields.CharField()
    load_id = fields.CharField()

    objects = RequestManager()

    class Meta:
        task_type = "TransportationTask"
        collection_name = TaskRequest.Meta.collection_name
        archive_collection = TaskRequest.Meta.archive_collection

    @classmethod
    def from_payload(cls, payload):
        document = super().to_document(payload)
        if "earliest_pickup_time" not in document:
            document["earliest_pickup_time"] = Timepoint(TimeStamp(tz=pytz.UTC).to_datetime(), 0)
        else:
            document["earliest_pickup_time"] = Timepoint.from_payload(document.pop("earliest_pickup_time"))

        if "latest_pickup_time" not in document:
            document["latest_pickup_time"] = Timepoint(TimeStamp(tz=pytz.UTC, delta=timedelta(minutes=10)).to_datetime(), 0)
        else:
            document["latest_pickup_time"] = Timepoint.from_payload(document.pop("latest_pickup_time"))
        request = cls.from_document(document)
        request.save()
        return request

    def from_recurring_event(self, event, user_id=None):
        request_type = self.__class__.__name__
        request_cls = getattr(this_module, request_type)
        kwargs = super().get_common_attrs()

        timezone_offset = event.start.utcoffset().total_seconds()/60
        earliest_pickup_time = Timepoint(event.start.astimezone(pytz.utc), timezone_offset)
        latest_pickup_time = Timepoint(event.start.astimezone(pytz.utc), timezone_offset)
        latest_pickup_time.postpone(event.start_delta)

        kwargs.update({"event": event.uid,
                       "pickup_location":event.pickup_location,
                       "delivery_location":event.delivery_location,
                       "earliest_pickup_time":earliest_pickup_time,
                       "latest_pickup_time": latest_pickup_time,
                       "load_type": event.load_type,
                       "load_id": event.load_id})
        if user_id:
            kwargs.update(user_id=user_id)

        request = request_cls.create_new(**kwargs)
        return request

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
                    'finish_location': 'delivery_location',
                    'earliest_start_time': 'earliest_pickup_time',
                    'latest_start_time': 'latest_pickup_time'}
        updated_kwargs = dict()
        for key, value in kwargs.items():
            if key in map_keys:
                updated_kwargs[map_keys[key]] = value
            else:
                updated_kwargs[key] = value
        return updated_kwargs

    @classmethod
    def parse_dict(cls, **kwargs):
        kwargs = super().parse_dict(**kwargs)
        if "earliest_pickup_time" in kwargs:
            kwargs["earliest_pickup_time"] = Timepoint.from_payload(kwargs.pop("earliest_pickup_time"))

        if "latest_pickup_time" in kwargs:
            kwargs["latest_pickup_time"] = Timepoint.from_payload(kwargs.pop("latest_pickup_time"))
        return kwargs


class NavigationRequest(TaskRequest):

    start_location = fields.EmbeddedDocumentField(Position)
    start_location_level = fields.IntegerField()
    waypoints = fields.EmbeddedDocumentListField(Position, blank=True)
    goal_location = fields.EmbeddedDocumentField(Position)
    goal_location_level = fields.IntegerField()
    earliest_arrival_time = fields.EmbeddedDocumentField(Timepoint)
    latest_arrival_time = fields.EmbeddedDocumentField(Timepoint)
    wait_at_goal = fields.IntegerField(default=0)  # seconds

    objects = RequestManager()

    class Meta:
        task_type = "NavigationTask"
        collection_name = TaskRequest.Meta.collection_name
        archive_collection = TaskRequest.Meta.archive_collection

    @classmethod
    def from_payload(cls, payload):
        document = super().to_document(payload)
        if "earliest_arrival_time" not in document:
            document["earliest_arrival_time"] = Timepoint(TimeStamp(tz=pytz.UTC).to_datetime(), 0)
        else:
            document["earliest_arrival_time"] = Timepoint.from_payload(document.pop("earliest_arrival_time"))

        if "latest_arrival_time" not in document:
            document["latest_arrival_time"] = Timepoint(TimeStamp(tz=pytz.UTC, delta=timedelta(minutes=10)).to_datetime(), 0)
        else:
            document["latest_arrival_time"] = Timepoint.from_payload(document.pop("latest_arrival_time"))
        request = cls.from_document(document)
        request.save()
        return request

    def from_recurring_event(self, event, user_id=None):
        request_type = self.__class__.__name__
        request_cls = getattr(this_module, request_type)
        kwargs = super().get_common_attrs()

        timezone_offset = event.start.utcoffset().total_seconds()/60
        earliest_arrival_time = Timepoint(event.start.astimezone(pytz.utc), timezone_offset)
        latest_arrival_time = Timepoint(event.start.astimezone(pytz.utc), timezone_offset)
        latest_arrival_time.postpone(event.start_delta)

        kwargs.update({"event": event.uid,
                       "start_location": event.start_location,
                       "goal_location": event.goal_location,
                       "earliest_arrival_time": earliest_arrival_time,
                       "latest_arrival_time": latest_arrival_time,
                       "wait_at_goal": event.wait_at_goal})
        if user_id:
            kwargs.update(user_id=user_id)

        request = request_cls.create_new(**kwargs)
        return request

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
        map_keys = {'earliest_start_time': 'earliest_arrival_time',
                    'latest_start_time': 'latest_arrival_time'}
        updated_kwargs = dict()
        for key, value in kwargs.items():
            if key in map_keys:
                updated_kwargs[map_keys[key]] = value
            else:
                updated_kwargs[key] = value
        return updated_kwargs

    @classmethod
    def parse_dict(cls, **kwargs):
        kwargs = super().parse_dict(**kwargs)
        if "earliest_arrival_time" in kwargs:
            kwargs["earliest_arrival_time"] = Timepoint.from_payload(kwargs.pop("earliest_arrival_time"))

        if "latest_arrival_time" in kwargs:
            kwargs["latest_arrival_time"] = Timepoint.from_payload(kwargs.pop("latest_arrival_time"))
        return kwargs


class DefaultNavigationRequest(NavigationRequest):
    """ Send robot to its waiting location
    """
    robot = fields.ReferenceField(Robot)
    objects = RequestManager()

    class Meta:
        task_type = "DefaultNavigationTask"
        collection_name = TaskRequest.Meta.collection_name
        archive_collection = TaskRequest.Meta.archive_collection


class ChargingRequestQuerySet(RequestQuerySet):

    def by_robot(self, robot_id):
        requests = [r for r in self.raw({"robot": robot_id})]
        invalid_requests = list()
        for r in requests:
            try:
                self.validate_model(r)
            except ValidationError:
                invalid_requests.append(r)
        return [r for r in requests if r not in invalid_requests]


ChargingRequestManager = Manager.from_queryset(ChargingRequestQuerySet)


class ChargingRequest(TaskRequest):
    """ Send robot to a charging station and charge until the given charging percentage
    """
    robot = fields.ReferenceField(Robot)
    charging_percentage = fields.FloatField(default=100)
    earliest_start_time = fields.EmbeddedDocumentField(Timepoint)
    latest_start_time = fields.EmbeddedDocumentField(Timepoint)
    priority = fields.IntegerField(default=TaskPriority.HIGH)

    objects = ChargingRequestManager()

    class Meta:
        task_type = "ChargingTask"
        collection_name = TaskRequest.Meta.collection_name
        archive_collection = TaskRequest.Meta.archive_collection

    @classmethod
    def get_task_requests_by_robot(cls, robot_id):
        return cls.objects.by_robot(robot_id)

    @staticmethod
    def map_args(**kwargs):
        # Locations are taken from the charging station (filled it in the task)
        try:
            kwargs.pop("start_location")
        except KeyError:
            pass
        try:
            kwargs.pop("finish_location")
        except KeyError:
            pass
        return kwargs

    def validate_request(self, path_planner, complete_request=True):
        if self.latest_start_time.utc_time < TimeStamp().to_datetime():
            raise InvalidRequestTime("Latest start time %s is in the past" % self.latest_start_time)
        if self.latest_start_time < self.earliest_start_time:
            raise InvalidRequestTime("Latest start time %s is earlier than the earliest start time %s",
                                     self.latest_start_time, self.earliest_start_time)
        if self.is_repetitive():
            raise InvalidRequest("Charging request cannot be repetitive")

    @classmethod
    def from_payload(cls, payload):
        document = super().to_document(payload)
        if "earliest_start_time" not in document:
            document["earliest_start_time"] = Timepoint(TimeStamp(tz=pytz.UTC).to_datetime(), 0)
        else:
            document["earliest_start_time"] = Timepoint.from_payload(document.pop("earliest_start_time"))

        if "latest_start_time" not in document:
            document["latest_start_time"] = Timepoint(TimeStamp(tz=pytz.UTC, delta=timedelta(minutes=10)).to_datetime(), 0)
        else:
            document["latest_start_time"] = Timepoint.from_payload(document.pop("latest_start_time"))
        request = cls.from_document(document)
        request.save()
        return request

    def to_dict(self):
        dict_repr = super().to_dict()
        dict_repr["earliest_start_time"] = self.earliest_start_time.to_dict()
        dict_repr["latest_start_time"] = self.latest_start_time.to_dict()
        return dict_repr


class GuidanceRequest(NavigationRequest):
    objects = RequestManager()

    class Meta:
        task_type = "GuidanceTask"
        collection_name = TaskRequest.Meta.collection_name
        archive_collection = TaskRequest.Meta.archive_collection


class DisinfectionRequest(TaskRequest):
    area = fields.CharField()
    start_location = fields.EmbeddedDocumentField(Position)
    finish_location = fields.EmbeddedDocumentField(Position)
    earliest_start_time = fields.EmbeddedDocumentField(Timepoint)
    latest_start_time = fields.EmbeddedDocumentField(Timepoint)
    dose = fields.IntegerField(default=DisinfectionDose.NORMAL)

    objects = RequestManager()

    class Meta:
        task_type = "DisinfectionTask"
        collection_name = TaskRequest.Meta.collection_name
        archive_collection = TaskRequest.Meta.archive_collection

    @classmethod
    def from_payload(cls, payload):
        document = super().to_document(payload)
        if "earliest_start_time" not in document:
            document["earliest_start_time"] = Timepoint(TimeStamp(tz=pytz.UTC).to_datetime(), 0)
        else:
            document["earliest_start_time"] = Timepoint.from_payload(document.pop("earliest_start_time"))

        if "latest_start_time" not in document:
            document["latest_start_time"] = Timepoint(TimeStamp(tz=pytz.UTC, delta=timedelta(minutes=10)).to_datetime(), 0)
        else:
            document["latest_start_time"] = Timepoint.from_payload(document.pop("latest_start_time"))
        request = cls.from_document(document)
        request.save()
        return request

    def from_recurring_event(self, event, repetition_pattern=None, user_id=None):
        request_type = self.__class__.__name__
        request_cls = getattr(this_module, request_type)
        kwargs = super().get_common_attrs()

        timezone_offset = event.start.utcoffset().total_seconds()/60
        earliest_start_time = Timepoint(event.start.astimezone(pytz.utc), timezone_offset)
        latest_start_time = Timepoint(event.start.astimezone(pytz.utc), timezone_offset)
        latest_start_time.postpone(event.start_delta)

        kwargs.update({"event": event.uid,
                       "area": event.area,
                       "earliest_start_time": earliest_start_time,
                       "latest_start_time": latest_start_time})
        if repetition_pattern:
            kwargs.update(repetition_pattern=repetition_pattern)
        if user_id:
            kwargs.update(user_id=user_id)

        request = request_cls.create_new(**kwargs)
        return request

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
        if not path_planner.is_valid_area(self.map, self.area):
            raise InvalidRequestArea("%s is not a valid area." % self.area)
        if complete_request:
            self.complete_request(path_planner)
        super().validate_request(path_planner, complete_request)

    def complete_request(self, path_planner):
        self.start_location = path_planner.get_start_location(self.map, self.area)
        self.finish_location = path_planner.get_finish_location(self.map, self.area)
        self.save()

    def to_dict(self):
        dict_repr = super().to_dict()
        dict_repr["earliest_start_time"] = self.earliest_start_time.to_dict()
        dict_repr["latest_start_time"] = self.latest_start_time.to_dict()
        return dict_repr

    @classmethod
    def parse_dict(cls, **kwargs):
        kwargs = super().parse_dict(**kwargs)
        if "earliest_start_time" in kwargs:
            kwargs["earliest_start_time"] = Timepoint.from_payload(kwargs.pop("earliest_start_time"))

        if "latest_start_time" in kwargs:
            kwargs["latest_start_time"] = Timepoint.from_payload(kwargs.pop("latest_start_time"))
        return kwargs


class InvalidRequest(Exception):
    pass

class InvalidRequestMap(InvalidRequest):
    pass

class InvalidRequestLocation(InvalidRequest):
    pass


class InvalidRequestArea(InvalidRequest):
    pass


class InvalidRequestTime(InvalidRequest):
    pass


class TaskRequestUpdate(TaskRequest):
    task_id = fields.UUIDField()
    update_all = fields.BooleanField(default=False)

    class Meta:
        collection_name = "task_request_update"
        archive_collection = "task_request_update_archive"
        ignore_unknown_fields = True


class TransportationRequestUpdate(TransportationRequest, TaskRequestUpdate):

    class Meta:
        collection_name = TaskRequestUpdate.Meta.collection_name
        archive_collection = TaskRequestUpdate.Meta.archive_collection
        ignore_unknown_fields = TaskRequestUpdate.Meta.ignore_unknown_fields
        task_type = TransportationRequest.Meta.task_type


class NavigationRequestUpdate(NavigationRequest, TaskRequestUpdate):

    class Meta:
        collection_name = TaskRequestUpdate.Meta.collection_name
        archive_collection = TaskRequestUpdate.Meta.archive_collection
        ignore_unknown_fields = TaskRequestUpdate.Meta.ignore_unknown_fields
        task_type = NavigationRequest.Meta.task_type


class DefaultNavigationRequestUpdate(NavigationRequestUpdate):

    class Meta:
        collection_name = TaskRequestUpdate.Meta.collection_name
        archive_collection = TaskRequestUpdate.Meta.archive_collection
        ignore_unknown_fields = TaskRequestUpdate.Meta.ignore_unknown_fields
        task_type = DefaultNavigationRequest.Meta.task_type


class GuidanceRequestUpdate(NavigationRequestUpdate):

    class Meta:
        collection_name = TaskRequestUpdate.Meta.collection_name
        archive_collection = TaskRequestUpdate.Meta.archive_collection
        ignore_unknown_fields = TaskRequestUpdate.Meta.ignore_unknown_fields
        task_type = GuidanceRequest.Meta.task_type


class DisinfectionRequestUpdate(DisinfectionRequest, TaskRequestUpdate):

    class Meta:
        collection_name = TaskRequestUpdate.Meta.collection_name
        archive_collection = TaskRequestUpdate.Meta.archive_collection
        ignore_unknown_fields = TaskRequestUpdate.Meta.ignore_unknown_fields
        task_type = DisinfectionRequest.Meta.task_type



