from fmlib.models.environment import Position
from pymodm import fields, MongoModel
from pymodm.context_managers import switch_collection
from pymodm.errors import ValidationError
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from ropod.structs.status import AvailabilityStatus


class WaitingLocationQuerySet(QuerySet):

    def validate_model(self, waiting_location):
        try:
            waiting_location.full_clean()
            return waiting_location
        except ValidationError:
            print(f"waiting location{waiting_location.location_name} has a deprecated format")
            waiting_location.deprecate()
            raise

    def get_waiting_location(self, location_name):
        waiting_location = self.get({'_id': location_name})
        return self.validate_model(waiting_location)

    def by_status(self, status):
        waiting_locations = [c for c in self.raw({"status": status})]
        invalid = list()
        for w in waiting_locations:
            try:
                self.validate_model(w)
            except ValidationError:
                invalid.append(w)
        return [w for w in waiting_locations if w not in invalid]


WaitingLocationManager = Manager.from_queryset(WaitingLocationQuerySet)


class WaitingLocation(MongoModel):
    location_name = fields.CharField(primary_key=True)
    position = fields.EmbeddedDocumentField(Position)
    status = fields.IntegerField(default=AvailabilityStatus.IDLE)

    objects = WaitingLocationManager()

    class Meta:
        archive_collection = 'waiting_location_archive'
        ignore_unknown_fields = True

    def archive(self):
        with switch_collection(self, self.Meta.archive_collection):
            super().save()
        self.delete()

    def deprecate(self):
        """The waiting location has a deprecated format. Remove it from the "waiting_location" collection and store it
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
    def create_new(cls, location_name, **kwargs):
        if 'position' not in kwargs.keys():
            kwargs.update(position=Position())
        waiting_location = cls(location_name, **kwargs)
        waiting_location.save()
        return waiting_location

    @classmethod
    def get_waiting_location(cls, location_name):
        return cls.objects.get_waiting_location(location_name)

    @classmethod
    def by_status(cls, status):
        return cls.objects.by_status(status)

    @classmethod
    def get_all(cls):
        waiting_locations = cls.objects.all()
        deprecated = list()
        for w in waiting_locations:
            try:
                cls.objects.validate_model(w)
            except ValueError:
                deprecated.append(w)
        return [w for w in waiting_locations if w not in deprecated]

    def update_availability(self, availability_status):
        self.status = availability_status
        self.save()

