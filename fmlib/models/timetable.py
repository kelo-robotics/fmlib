import logging
import uuid
from datetime import timedelta

import dateutil.parser
from fmlib.utils.messages import Document
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
    solver_name = fields.CharField()
    ztp = fields.DateTimeField()
    stn = fields.DictField()
    dispatchable_graph = fields.DictField(default=dict())
    stn_tasks = fields.DictField(blank=True)

    objects = TimetableManager()

    class Meta:
        archive_collection = 'timetable_archive'
        ignore_unknown_fields = True

    def save(self):
        try:
            super().save(cascade=True)
        except ServerSelectionTimeoutError:
            logging.warning('Could not save models to MongoDB')

    @classmethod
    def get_timetable(cls, robot_id):
        return cls.objects.get_timetable(robot_id)

    def get_node_id(self, task_id, node_type):
        if isinstance(task_id, uuid.UUID):
            task_id = str(task_id)
        for i, node in enumerate(self.dispatchable_graph["nodes"]):
            if task_id == node["data"]["task_id"] and node_type == node["data"]["node_type"]:
                return i

    def get_time(self, task_id, node_type, lower_bound=True):
        time_ = None
        node_id = self.get_node_id(task_id, node_type)
        for link in self.dispatchable_graph["links"]:
            if lower_bound and link["source"] == node_id and link["target"] == 0:
                time_ = -link["weight"]
                break
            if not lower_bound and link["source"] == 0 and link["target"] == node_id:
                time_ = link["weight"]
                break
        if time_:
            return self.ztp + timedelta(seconds=time_)

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        document['_id'] = document.pop('robot_id')
        document["ztp"] = dateutil.parser.parse(document.pop("ztp"))
        timetable = Timetable.from_document(document)
        return timetable

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr["robot_id"] = str(dict_repr.pop('_id'))
        dict_repr["ztp"] = self.ztp.isoformat()
        return dict_repr
