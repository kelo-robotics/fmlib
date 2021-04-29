import codecs
import logging
import pickle

from pymodm import fields, MongoModel
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from pymongo.errors import ServerSelectionTimeoutError


class TimetableQuerySet(QuerySet):
    def get_timetable(self, robot_id):
        """ Returns a timetable mongo model that matches to the robot_id
        """
        return self.get({'_id': robot_id})


TimetableManager = Manager.from_queryset(TimetableQuerySet)


class Timetable(MongoModel):
    robot_id = fields.IntegerField(primary_key=True)
    data = fields.CharField()

    objects = TimetableManager()

    class Meta:
        archive_collection = 'timetable_archive'
        ignore_unknown_fields = True
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
    def from_obj(cls, obj):
        robot_id = obj.robot.robot_id
        data = codecs.encode(pickle.dumps(obj, 2), "base64").decode()
        timetable = cls(robot_id=robot_id, data=data)
        timetable.save()
