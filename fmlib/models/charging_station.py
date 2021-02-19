from fmlib.models.environment import Position
from pymodm import fields, MongoModel
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from ropod.structs.status import AvailabilityStatus


class ChargingStationQuerySet(QuerySet):

    def validate_model(self, charging_station):
        try:
            charging_station.full_clean()
            return charging_station
        except ValidationError:
            print(f"Charging station {charging_station.station_id} has a deprecated format")
            charging_station.deprecate()
            raise

    def get_charging_station(self, station_id):
        charging_station = self.get({'_id': station_id})
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
    station_id = fields.IntegerField(primary_key=True)
    position = fields.EmbeddedDocumentField(Position)
    status = fields.IntegerField(default=AvailabilityStatus.IDLE)

    objects = ChargingStationManager()

    class Meta:
        archive_collection = 'charging_station_archive'
        ignore_unknown_fields = True

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
    def create_new(cls, station_id, **kwargs):
        if 'position' not in kwargs.keys():
            kwargs.update(position=Position())
        charging_station = cls(station_id, **kwargs)
        charging_station.save()
        return charging_station

    @classmethod
    def get_charging_station(cls, station_id):
        return cls.objects.get_charging_station(station_id)

    @classmethod
    def by_status(cls, status):
        return cls.objects.by_status(status)

    @classmethod
    def get_all(cls):
        charging_stations = cls.objects.all()
        deprecated = list()
        for c in charging_stations:
            try:
                cls.objects.validate_model(c)
            except ValueError:
                deprecated.aapend(c)
        return [c for c in charging_stations if c not in deprecated]

    def update_availability(self, availability_status):
        self.status = availability_status
        self.save()
