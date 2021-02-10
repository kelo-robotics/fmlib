import logging

from pymodm import EmbeddedMongoModel, fields, MongoModel
from pymodm.context_managers import switch_collection
from pymodm.errors import ValidationError
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from pymongo.errors import ServerSelectionTimeoutError
from ropod.structs.status import AvailabilityStatus

from fmlib.models.environment import Position


class Availability(EmbeddedMongoModel):

    status = fields.IntegerField(default=AvailabilityStatus.NO_COMMUNICATION, blank=True)
    current_task = fields.UUIDField(default=None, blank=True)

    class Meta:
        cascade = True

    def update_status(self, availability_status, current_task):
        self.status = availability_status
        self.current_task = current_task


class RobotStatus(EmbeddedMongoModel):

    availability = fields.EmbeddedDocumentField(Availability)
    battery = fields.FloatField()


class HardwareComponent(EmbeddedMongoModel):

    uuid = fields.UUIDField(primary_key=True)
    id = fields.CharField()
    model = fields.CharField()
    serial_number = fields.CharField(default='unknown')
    firmware_version = fields.CharField(default='unknown')
    version = fields.CharField(default='unknown')


class Wheel(HardwareComponent):
    pass


class Laser(HardwareComponent):
    pass


class RobotHardware(MongoModel):

    wheels = fields.EmbeddedDocumentListField(Wheel)
    laser = fields.EmbeddedDocumentListField(Laser)


class SoftwareComponent(EmbeddedMongoModel):

    name = fields.CharField(primary_key=True)
    package = fields.CharField()
    version = fields.CharField()
    version_uid = fields.CharField()
    update_available = fields.BooleanField()
    config_mismatch = fields.BooleanField()
    uncommitted_changes = fields.BooleanField()


class SoftwareStack(MongoModel):

    navigation_stack = fields.EmbeddedDocumentListField(SoftwareComponent)
    interfaces = fields.EmbeddedDocumentListField(SoftwareComponent)


class Version(EmbeddedMongoModel):

    hardware = fields.EmbeddedDocumentField(RobotHardware)
    software = fields.EmbeddedDocumentField(SoftwareStack)


class RobotQuerySet(QuerySet):

    def validate_model(self, robot):
        try:
            robot.full_clean()
            return robot
        except ValidationError:
            print(f"Robot {robot.robot_id} has a deprecated format")
            robot.deprecate()
            raise

    def get_robot(self, robot_id):
        robot = self.get({'_id': robot_id})
        return self.validate_model(robot)


RobotManager = Manager.from_queryset(RobotQuerySet)


class Robot(MongoModel):

    robot_id = fields.IntegerField(primary_key=True)
    version = fields.EmbeddedDocumentField(Version)
    status = fields.EmbeddedDocumentField(RobotStatus)
    position = fields.EmbeddedDocumentField(Position)
    capabilities = fields.ListField(default=["navigation"])

    objects = RobotManager()

    class Meta:
        archive_collection = 'robot_archive'
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
        """The robot has a deprecated format. Remove the robot from the "robot" collection and store it in the
        robot_archive collection. Only the fields that remain valid are stored in the archive_collection."""

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
    def get_robot(cls, robot_id):
        return cls.objects.get_robot(robot_id)

    @classmethod
    def get_robots(cls):
        return cls.objects.all()

    @classmethod
    def get_robots_by_availability(cls, availability_status):
        robots = cls.get_robots()
        return [robot for robot in robots if robot.status.availability.status == availability_status]

    def update_version(self, software, hardware):
        self.version.software = software
        self.version.hardware = hardware
        self.save()

    def update_position(self, **kwargs):
        save_in_db = kwargs.pop("save_in_db", True)
        self.position.update_position(**kwargs)
        if save_in_db:
            self.save()

    def update_availability(self, availability_status, current_task=None):
        self.status.availability.update_status(availability_status, current_task)
        self.save()

    def update_battery(self, battery):
        self.status.battery = battery
        self.save()

    def is_capable(self, task):
        return all(i in self.capabilities for i in task.capabilities)

    def is_eligible(self, task):
        if self.is_capable(task) and self.position.map == task.request.map:
            if task.request.eligible_robots and self.robot_id in task.request.eligible_robots:
                return True
            elif not task.request.eligible_robots:
                return True
        return False

    @classmethod
    def create_new(cls, robot_id, **kwargs):
        save_in_db = kwargs.pop("save_in_db", True)
        if 'position' not in kwargs.keys():
            kwargs.update(position=Position())

        robot = cls(robot_id, **kwargs)
        robot.status = RobotStatus(availability=Availability())

        if save_in_db:
            robot.save()
        return robot

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr["robot_id"] = str(dict_repr.pop('_id'))
        return dict_repr
