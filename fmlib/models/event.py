import datetime
import uuid

import dateutil.parser
from fmlib.utils.messages import Document
from icalendar import Calendar, Event as ICalendarEvent
from icalendar.prop import vRecur, vDDDTypes
from pymodm import fields, MongoModel
from pymodm.context_managers import switch_collection
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from pymongo.errors import ServerSelectionTimeoutError


class EventQuerySet(QuerySet):

    def get_event(self, uid):
        if isinstance(uid, str):
            try:
                uid = uuid.UUID(uid)
            except ValueError as e:
                raise e

        return self.get({'_id': uid})

EventManager = Manager.from_queryset(EventQuerySet)

class Event(MongoModel):
    uid = fields.UUIDField(primary_key=True)
    rrule = fields.DictField(blank=True)  # recurrent rule to create a recurrent event
    exdate = fields.ListField(blank=True)  # list of dates to exclude from the rrule
    summary = fields.CharField(default="Recurrent event")  # Description of the task (to be shown in the calendar)
    dtstart = fields.DateTimeField()
    dtend = fields.DateTimeField()
    dtstart_delta = fields.FloatField()
    task_type = fields.CharField()

    objects = EventManager()

    class Meta:
        archive_collection = "event_archive"
        ignore_unknown_fields = True

    def save(self):
        try:
            super().save(cascade=True)
        except ServerSelectionTimeoutError:
            logging.warning('Could not save models to MongoDB')

    def archive(self):
        with switch_collection(self, Event.Meta.archive_collection):
            super().save()
        self.delete()

    @classmethod
    def create_new(cls, **kwargs):
        uid = kwargs.get("uid")
        if uid:
            if isinstance(uid, str):
                kwargs.update(uid=uuid.UUID(uid))
        else:
            kwargs.update(uid=uuid.uuid4())

        exdate = kwargs.get("exdate")
        if exdate:
            parsed_exdate = list()
            for d in exdate:
                if isinstance(d, str):
                    parsed_exdate.append(dateutil.parser.parse(d))
                elif isinstance(d, datetime.date):
                    parsed_exdate.append(dateutil.parser.parse(d.isoformat()))
                elif isinstance(d, datetime.datetime):
                    parsed_exdate.append(d)
            kwargs.update(exdate=parsed_exdate)

        event = cls(**kwargs)
        event.save()
        return event

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        if "uid" not in document.keys():
            document["uid"] = uuid.uuid4()
        document['_id'] = document.pop("uid")
        exdate = document.get("exdate")
        if exdate:
            parsed_exdate = list()
            for d in exdate:
                parsed_exdate.append(dateutil.parser.parse(d))
            document["exdate"] = parsed_exdate
        event = cls.from_document(document)
        event.save()
        return event

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr["uid"] = str(dict_repr.pop('_id'))
        if dict_repr.get("exdate"):
            parsed_exdate = list()
            for d in dict_repr["exdate"]:
                parsed_exdate.append(d.isoformat())
            dict_repr["exdate"] = parsed_exdate
        return dict_repr

    @classmethod
    def parse_dict(cls, **kwargs):
        if "exdate" in kwargs:
            parsed_exdate = list()
            for d in kwargs.get("exdates"):
                parsed_exdate.append(dateutil.parser.parse(d))
            kwargs.update(exdates=parsed_exdate)
        return kwargs

    def to_icalendar_event(self, **kwargs):
        event = ICalendarEvent()
        event.add('rrule', self.rrule)

        for exdate in self.exdate:
            event.add('exdate', exdate)

        event.add('summary', self.summary)
        event.add('uid', self.uid)
        event.add('task-type', self.task_type)

        for key, value in kwargs.items():

            event.add(key.replace('_', '-'), value)

        return event

    @classmethod
    def from_icalendar_event(cls, event):
        cal = Calendar()

        kwargs = dict()
        largest_exdate_list = list()
        for p in event.property_items():
            (name, value) = p
            name = name.lower().replace('-', '_')
            cal.add(name, value)
            value = cal.decoded(name)

            if isinstance(value, bytes):
                value = value.decode("utf-8")

            if isinstance(value, vRecur):
                parsed_value = dict()
                for key, val in value.items():
                    parsed_value[key] = val.pop()
                value = parsed_value

            if name == "exdate" and isinstance(value, list):
                if len(value) > len(largest_exdate_list):
                    largest_exdate_list = value

            if name in cls.__dict__:
                kwargs[name] = value

        if largest_exdate_list:
            parsed_value = list()
            for dts in largest_exdate_list:
                for dt in dts.dts:
                    parsed_value.append(vDDDTypes.from_ical(dt))
            kwargs['exdate'] = parsed_value

        cls.create_new(**kwargs)

    @classmethod
    def get_event(cls, uid):
        return cls.objects.get_event(uid)

