import logging

from fmlib.models.actions import Action
from fmlib.models.environment import Position
from fmlib.models.tasks import Task
from pymodm import EmbeddedMongoModel, fields, MongoModel
from pymodm.context_managers import switch_collection
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from pymongo.errors import ServerSelectionTimeoutError
from ropod.structs.status import AvailabilityStatus, ComponentStatus as ComponentStatusConst


class ComponentStatus(EmbeddedMongoModel):
    status = fields.IntegerField(default=ComponentStatusConst.OPTIMAL)
    issues = fields.DictField(blank=True)

    class Meta:
        cascade = True

    def update_status(self, health_status, issues=None):
        if issues is None:
            issues = dict()

        self.status = health_status
        self.issues = issues


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
    component = fields.EmbeddedDocumentField(ComponentStatus)


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

    def get_robot(self, robot_id):
        if isinstance(robot_id, int):
            robot_id = str(robot_id)

        return self.get({'_id': robot_id})


RobotManager = Manager.from_queryset(RobotQuerySet)


class Robot(MongoModel):

    robot_id = fields.CharField(primary_key=True)
    uuid = fields.UUIDField()
    version = fields.EmbeddedDocumentField(Version)
    status = fields.EmbeddedDocumentField(RobotStatus)
    position = fields.EmbeddedDocumentField(Position)

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

    @staticmethod
    def get_robot(robot_id):
        return Robot.objects.get_robot(robot_id)

    def update_position(self, save=True, **kwargs):
        self.position.update_2d_pose(**kwargs)
        if save:
            self.save()

    def update_component_status(self, health_status, issues=None):
        self.status.component.update_status(health_status, issues)
        self.save()

    def update_availability(self, availability_status, current_task=None):
        self.status.availability.update_status(availability_status, current_task)
        self.save()

    @classmethod
    def create_new(cls, robot_id, save=True, **kwargs):
        if 'position' not in kwargs.keys():
            kwargs.update(position=Position())

        robot = cls(robot_id, **kwargs)
        if save:
            robot.save()
        return robot

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr["robot_id"] = str(dict_repr.pop('_id'))
        return dict_repr
