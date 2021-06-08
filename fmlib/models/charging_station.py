import logging

from fmlib.models.environment import Position
from fmlib.models.timetable import Timetable
from pymodm import fields, MongoModel
from pymodm.context_managers import switch_collection
from pymodm.errors import ValidationError
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from pymongo.errors import ServerSelectionTimeoutError
from ropod.structs.status import AvailabilityStatus


class ChargingStationQuerySet(QuerySet):

    def validate_model(self, charging_station):
        try:
            charging_station.full_clean()
            return charging_station
        except ValidationError:
            print(f"Charging station {charging_station.station_name} has a deprecated format")
            charging_station.deprecate()
            raise

    def get_charging_station(self, station_name):
        charging_station = self.get({'_id': station_name})
        return self.validate_model(charging_station)

    def by_status(self, status):
        charging_stations = [c for c in self.raw({"status": status})]
        invalid = list()
        for c in charging_stations:
            try:
                self.validate_model(c)
            except ValidationError:
                invalid.append(c)
        return [c for c in charging_stations if c not in invalid]


ChargingStationManager = Manager.from_queryset(ChargingStationQuerySet)


class ChargingStation(MongoModel):
    station_name = fields.CharField(primary_key=True)
    timetable = fields.EmbeddedDocumentField(Timetable)
    archived_timetable = fields.EmbeddedDocumentField(Timetable)
    position = fields.EmbeddedDocumentField(Position)
    status = fields.IntegerField(default=AvailabilityStatus.IDLE)

    objects = ChargingStationManager()

    class Meta:
        archive_collection = 'charging_station_archive'
        ignore_unknown_fields = True

    def save(self):
        try:
            super().save(cascade=True)
        except ServerSelectionTimeoutError:
            logging.warning('Could not save models to MongoDB')

    def archive(self):
        with switch_collection(self, self.Meta.archive_collection):
            super().save()
        self.delete()

    def deprecate(self):
        """The charging station has a deprecated format. Remove it from the "charging_station" collection and store it
        in the archive collection. Only the fields that remain valid are stored in the archive collection."""

        for field in self._mongometa.get_fields():
            try:
                field_value = field.value_from_object(self)
                field_empty = field.is_undefined(self)
                if field_empty and field.required:
                    setattr(self, field.attname, None)
                elif not field_empty:
                    field.validate(field_value)
            except Exception as exc:
                delattr(self, field.attname)

        self.archive()

    @classmethod
    def create_new(cls, station_name, **kwargs):
        if 'position' not in kwargs.keys():
            kwargs.update(position=Position())
        charging_station = cls(station_name, **kwargs)
        charging_station.save()
        return charging_station

    @classmethod
    def get_charging_station(cls, station_name):
        return cls.objects.get_charging_station(station_name)

    @classmethod
    def by_status(cls, status):
        return cls.objects.by_status(status)

    def get_timetable(self):
        if self.timetable:
            return self.timetable.get_timetable()

    def get_archived_timetable(self):
        if self.archived_timetable:
            return self.archived_timetable.get_timetable()

    def update_timetable(self, obj):
        self.timetable = Timetable.from_obj(obj)
        self.save()

    def update_archived_timetable(self, obj):
        self.archived_timetable = Timetable.from_obj(obj)
        self.save()

    @classmethod
    def get_all(cls):
        charging_stations = cls.objects.all()
        deprecated = list()
        for c in charging_stations:
            try:
                cls.objects.validate_model(c)
            except ValueError:
                deprecated.append(c)
        return [c for c in charging_stations if c not in deprecated]

    def update_availability(self, availability_status):
        self.status = availability_status
        self.save()
