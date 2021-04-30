import codecs
import logging
import pickle

from pymodm import fields, MongoModel
from pymodm.context_managers import switch_collection
from pymodm.errors import DoesNotExist
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from pymongo.errors import ServerSelectionTimeoutError


class TimetableQuerySet(QuerySet):

    def get_timetable(self, robot_id):
        """ Returns a timetable mongo model that matches to the robot_id
        """
        try:
            return self.get({'_id': robot_id})
        except ValueError:
            print(f"Timetable {robot_id} has a deprecated format")
            raise DoesNotExist


TimetableManager = Manager.from_queryset(TimetableQuerySet)


class Timetable(MongoModel):
    robot_id = fields.IntegerField(primary_key=True)
    data = fields.CharField()

    objects = TimetableManager()

    class Meta:
        archive_collection = 'timetable_archive'
        meta_model = "timetable"

    @property
    def meta_model(self):
        return self.Meta.meta_model

    def save(self):
        try:
            super().save(cascade=True)
        except ServerSelectionTimeoutError:
            logging.warning('Could not save models to MongoDB')

    @classmethod
    def get_timetable(cls, robot_id):
        timetable = cls.objects.get_timetable(robot_id)
        # Convert to object
        return pickle.loads(codecs.decode(timetable.data.encode(), "base64"))

    @classmethod
    def get_archived_timetable(cls, robot_id):
        with switch_collection(cls, cls.Meta.archive_collection):
            return cls.get_timetable(robot_id)

    @classmethod
    def from_obj(cls, obj):
        robot_id = obj.robot.robot_id
        data = codecs.encode(pickle.dumps(obj, 2), "base64").decode()
        timetable = cls(robot_id=robot_id, data=data)
        return timetable

    @classmethod
    def save_obj(cls, obj):
        timetable = cls.from_obj(obj)
        timetable.save()

    @classmethod
    def archive_obj(cls, obj):
        archived_timetable = cls.from_obj(obj)
        with switch_collection(cls, cls.Meta.archive_collection):
            archived_timetable.save()
