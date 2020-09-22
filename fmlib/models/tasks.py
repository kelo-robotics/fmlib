import logging
import uuid
from datetime import datetime, timedelta

import dateutil.parser
from fmlib.models import requests
from fmlib.models.actions import Action, ActionProgress, Duration
from fmlib.models.timetable import Timetable
from fmlib.utils.messages import Document
from fmlib.utils.messages import Message
from fmlib.utils.messages import MessageFactory
from pymodm import EmbeddedMongoModel, fields, MongoModel
from pymodm.context_managers import switch_collection
from pymodm.errors import DoesNotExist
from pymodm.manager import Manager
from pymodm.queryset import QuerySet
from pymongo.errors import ServerSelectionTimeoutError
from ropod.structs.status import ActionStatus, TaskStatus as TaskStatusConst
from ropod.utils.timestamp import TimeStamp

mf = MessageFactory()


class TaskQuerySet(QuerySet):

    def get_task(self, task_id):
        """Return a task object matching to a task_id.
        """
        if isinstance(task_id, str):
            task_id = uuid.UUID(task_id)

        return self.get({'_id': task_id})


class TaskStatusQuerySet(QuerySet):

    def by_status(self, status):
        return self.raw({"status": status})

    def unallocated(self):
        return self.raw({"status": TaskStatusConst.UNALLOCATED})

    def allocated(self):
        return self.raw({"status": TaskStatusConst.ALLOCATED})

    def planned(self):
        return self.raw({"status": TaskStatusConst.PLANNED})

    def scheduled(self):
        return self.raw({"status": TaskStatusConst.SCHEDULED})

    def shipped(self):
        return self.raw({"status": TaskStatusConst.DISPATCHED})

    def ongoing(self):
        return self.raw({"status": TaskStatusConst.ONGOING})

    def completed(self):
        return self.raw({"status": TaskStatusConst.COMPLETED})

    def aborted(self):
        return self.raw({"status": TaskStatusConst.ABORTED})

    def failed(self):
        return self.raw({"status": TaskStatusConst.FAILED})

    def canceled(self):
        return self.raw({"status": TaskStatusConst.CANCELED})

    def preempted(self):
        return self.raw({"status": TaskStatusConst.PREEMPTED})


TaskManager = Manager.from_queryset(TaskQuerySet)
TaskStatusManager = Manager.from_queryset(TaskStatusQuerySet)


class TimepointConstraint(EmbeddedMongoModel):
    earliest_time = fields.DateTimeField()
    latest_time = fields.DateTimeField()

    def __str__(self):
        to_print = ""
        to_print += "[{}, {}]".format(self.earliest_time.isoformat(), self.latest_time.isoformat())
        return to_print

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        document["earliest_time"] = dateutil.parser.parse(document.pop("earliest_time"))
        document["latest_time"] = dateutil.parser.parse(document.pop("latest_time"))
        return cls.from_document(document)

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr["earliest_time"] = self.earliest_time.isoformat()
        dict_repr["latest_time"] = self.latest_time.isoformat()
        return dict_repr

    def update(self, earliest_time, latest_time):
        self.earliest_time = earliest_time
        self.latest_time = latest_time


class TemporalConstraints(EmbeddedMongoModel):
    start = fields.EmbeddedDocumentField(TimepointConstraint, blank=True)
    finish = fields.EmbeddedDocumentField(TimepointConstraint, blank=True)
    duration = fields.EmbeddedDocumentField(Duration)

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        document['start'] = TimepointConstraint.from_payload(document.get('start'))
        document['finish'] = TimepointConstraint.from_payload(document.get('finish'))
        document['duration'] = Duration.from_payload(document.get('duration'))
        temporal_constraints = cls.from_document(document)
        return temporal_constraints

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr['start'] = self.start.to_dict()
        dict_repr['finish'] = self.start.to_dict()
        dict_repr['duration'] = self.duration.to_dict()
        return dict_repr


class TaskConstraints(EmbeddedMongoModel):
    hard = fields.BooleanField(default=True)
    temporal = fields.EmbeddedDocumentField(TemporalConstraints)

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        document["temporal"] = TemporalConstraints.from_payload(document.pop("temporal"))
        task_constraints = cls.from_document(document)
        return task_constraints

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr['temporal'] = self.temporal.to_dict()
        return dict_repr


class TaskPlan(EmbeddedMongoModel):
    robot = fields.CharField(blank=True)
    actions = fields.EmbeddedDocumentListField(Action)


class Task(MongoModel):
    task_id = fields.UUIDField(primary_key=True)
    request = fields.EmbeddedDocumentField(requests.TaskRequest)
    assigned_robots = fields.ListField(blank=True)
    plan = fields.EmbeddedDocumentListField(TaskPlan, blank=True)
    constraints = fields.EmbeddedDocumentField(TaskConstraints)
    scheduled_time = fields.EmbeddedDocumentField(TimepointConstraint)

    objects = TaskManager()

    class Meta:
        archive_collection = 'task_archive'
        ignore_unknown_fields = True
        meta_model = 'task'

    def save(self):
        try:
            super().save(cascade=True)
        except ServerSelectionTimeoutError:
            logging.warning('Could not save models to MongoDB')

    def publish_task_update(self):
        msg = mf.create_message(self)
        msg.type = "TASK-UPDATE"
        self.api.publish(msg, groups=['ROPOD'])

    @classmethod
    def create_new(cls, **kwargs):
        if 'task_id' not in kwargs.keys():
            kwargs.update(task_id=uuid.uuid4())
        elif 'constraints' not in kwargs.keys():
            start = TimepointConstraint(earliest_time=datetime.now(),
                                        latest_time=datetime.now() + timedelta(minutes=1))
            temporal = TemporalConstraints(start=start, duration=Duration())
            kwargs.update(constraints=TaskConstraints(temporal=temporal))
        kwargs.update(scheduled_time=TimepointConstraint())
        task = cls(**kwargs)
        task.save()
        task.update_status(TaskStatusConst.UNALLOCATED)
        return task

    @property
    def departure_time(self):
        if self.assigned_robots:
            robot_id = self.assigned_robots[0]
            timetable = Timetable.get_timetable(robot_id)
            return timetable.get_time(self.task_id, "departure")

    @property
    def start_time(self):
        if self.assigned_robots:
            robot_id = self.assigned_robots[0]
            timetable = Timetable.get_timetable(robot_id)
            return timetable.get_time(self.task_id, "start")

    @property
    def finish_time(self):
        if self.assigned_robots:
            robot_id = self.assigned_robots[0]
            timetable = Timetable.get_timetable(robot_id)
            return timetable.get_time(self.task_id, "finish")

    @classmethod
    def from_payload(cls, payload, save_in_db=True):
        document = Document.from_payload(payload)
        document['_id'] = document.pop('task_id')
        if document.get("departure_time"):
            document["departure_time"] = dateutil.parser.parse(document.pop("departure_time"))
        if document.get("finish_time"):
            document["finish_time"] = dateutil.parser.parse(document.pop("finish_time"))
        task = cls.from_document(document)
        if save_in_db:
            task.save()
            task.update_status(TaskStatusConst.UNALLOCATED)

        return task

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr["task_id"] = str(dict_repr.pop('_id'))
        dict_repr["constraints"] = self.constraints.to_dict()
        if dict_repr.get("departure_time"):
            dict_repr["departure_time"] = self.departure_time.isoformat()
        if dict_repr.get("finish_time"):
            dict_repr["finish_time"] = self.finish_time.isoformat()
        dict_repr["status"] = self.status.status
        return dict_repr

    def to_msg(self):
        msg = Message.from_model(self)
        return msg

    @classmethod
    def from_request(cls, request, **kwargs):
        constraints = TaskConstraints(hard=request.hard_constraints)
        task = cls.create_new(request=request, constraints=constraints)
        return task

    @property
    def delayed(self):
        return self.status.delayed

    @delayed.setter
    def delayed(self, boolean):
        task_status = Task.get_task_status(self.task_id)
        task_status.delayed = boolean
        task_status.save()

    @property
    def hard_constraints(self):
        return self.constraints.hard

    @hard_constraints.setter
    def hard_constraints(self, boolean):
        self.constraints.hard = boolean
        self.save()

    @property
    def duration(self):
        return self.constraints.temporal.duration

    def update_duration(self, mean, variance):
        self.duration.update(mean, variance)
        self.save()

    @property
    def start_constraint(self):
        return self.constraints.temporal.start

    def update_start_constraint(self, earliest_time, latest_time, save_in_db=True):
        self.start_constraint.update(earliest_time, latest_time)
        if save_in_db:
            self.save()

    @classmethod
    def get_earliest_task(cls, tasks=None):
        if tasks is None:
            tasks = [task for task in cls.objects.all()]
        earliest_time = datetime.max
        earliest_task = None
        for task in tasks:
            if task.start_constraint.earliest_time < earliest_time:
                earliest_time = task.start_constraint.earliest_time
                earliest_task = task
        return earliest_task

    def archive(self):
        with switch_collection(self, Task.Meta.archive_collection):
            super().save()
        self.delete()

    def update_status(self, status):
        try:
            task_status = Task.get_task_status(self.task_id)
            task_status.status = status
        except DoesNotExist:
            task_status = TaskStatus(task=self.task_id, status=status)
        task_status.save()
        if status in [TaskStatusConst.COMPLETED,
                      TaskStatusConst.CANCELED,
                      TaskStatusConst.ABORTED,
                      TaskStatusConst.PREEMPTED]:
            task_status.archive()
            self.archive()

    def assign_robots(self, robot_ids, save_in_db=True,):
        self.assigned_robots = robot_ids
        # Assigns the first robot in the list to the plan
        # Does not work for single-task multi-robot
        self.plan[0].robot = robot_ids[0]
        if save_in_db:
            self.save()
            self.update_status(TaskStatusConst.ALLOCATED)
        self.publish_task_update()

    def unassign_robots(self):
        self.assigned_robots = list()
        self.plan[0].robot = None
        self.save()
        self.publish_task_update()

    def update_plan(self, task_plan):
        # Adds the section of the plan that is independent from the robot,
        # e.g., for transportation tasks, the plan between pickup and delivery
        self.plan.append(task_plan)
        self.update_status(TaskStatusConst.PLANNED)
        self.save()
        self.publish_task_update()

    def schedule(self, earliest_time, latest_time):
        self.update_status(TaskStatusConst.SCHEDULED)
        self.scheduled_time.update(earliest_time, latest_time)
        self.save()
        self.publish_task_update()

    def is_executable(self):
        current_time = TimeStamp()
        earliest_time = TimeStamp.from_datetime(self.scheduled_time.earliest_time)
        if earliest_time < current_time:
            return True
        else:
            return False

    def is_frozen(self):
        try:
            task_status = Task.get_task_status(self.task_id)
        except DoesNotExist:
            with switch_collection(Task, Task.Meta.archive_collection):
                task_status = Task.get_task_status(self.task_id)
        if task_status.status in [TaskStatusConst.SCHEDULED,
                                  TaskStatusConst.DISPATCHED,
                                  TaskStatusConst.ONGOING,
                                  TaskStatusConst.COMPLETED]:
            return True
        return False

    @property
    def meta_model(self):
        return self.Meta.meta_model

    @property
    def status(self):
        return TaskStatus.objects.get({"_id": self.task_id})

    @classmethod
    def get_task(cls, task_id):
        return cls.objects.get_task(task_id)


    @staticmethod
    def get_task_status(task_id):
        return TaskStatus.objects.get({'_id': task_id})


    @staticmethod
    def get_tasks_by_status(status):
        return [status.task for status in TaskStatus.objects.by_status(status)]


    @classmethod
    def get_tasks_by_robot(cls, robot_id):
        return [task for task in cls.objects.all() if robot_id in task.assigned_robots]


    @classmethod
    def get_tasks(cls, robot_id=None, status=None):
        if status:
            tasks = cls.get_tasks_by_status(status)
        else:
            tasks = [task for task in cls.objects.all()]

        if robot_id:
            tasks = [task for task in tasks if robot_id in task.assigned_robots]

        return tasks

    def update_progress(self, action_id, action_status, **kwargs):
        status = TaskStatus.objects.get({"_id": self.task_id})
        status.update_progress(action_id, action_status, **kwargs)


class TransportationTask(Task):
    request = fields.EmbeddedDocumentField(requests.TransportationRequest)
    capabilities = fields.ListField(default=["navigation", "docking"])

    objects = TaskManager()

    @classmethod
    def from_request(cls, request, **kwargs):
        api = kwargs.pop("api")
        pickup = TimepointConstraint(earliest_time=request.earliest_pickup_time,
                                     latest_time=request.latest_pickup_time)
        temporal = TemporalConstraints(start=pickup,
                                       duration=Duration())
        constraints = TaskConstraints(hard=request.hard_constraints, temporal=temporal)
        task = cls.create_new(request=request, constraints=constraints)
        if api:
            task.api = api
            task.publish_task_update()
        return task


class NavigationTask(Task):
    request = fields.EmbeddedDocumentField(requests.NavigationRequest)
    capabilities = fields.ListField(default=["navigation"])

    objects = TaskManager()

    @classmethod
    def from_request(cls, request, **kwargs):
        api = kwargs.pop("api")
        arrival = TimepointConstraint(earliest_time=request.earliest_arrival_time,
                                      latest_time=request.latest_arrival_time)
        temporal = TemporalConstraints(start=arrival,
                                       duration=Duration())
        constraints = TaskConstraints(hard=request.hard_constraints, temporal=temporal)
        task = cls.create_new(request=request, constraints=constraints)
        if api:
            task.api = api
            task.publish_task_update()
        return task


class GuidanceTask(Task):
    request = fields.EmbeddedDocumentField(requests.GuidanceRequest)
    capabilities = fields.ListField(default=["navigation", "guidance"])

    objects = TaskManager()

    @classmethod
    def from_request(cls, request, **kwargs):
        api = kwargs.pop("api")
        arrival = TimepointConstraint(earliest_time=request.earliest_arrival_time,
                                      latest_time=request.latest_arrival_time)
        temporal = TemporalConstraints(start=arrival,
                                       duration=Duration())
        constraints = TaskConstraints(hard=request.hard_constraints, temporal=temporal)
        task = cls.create_new(request=request, constraints=constraints)
        if api:
            task.api = api
            task.publish_task_update()
        return task


class DisinfectionTask(Task):
    request = fields.EmbeddedDocumentField(requests.DisinfectionRequest)
    capabilities = fields.ListField(default=["navigation", "uvc-radiation"])

    objects = TaskManager()

    @classmethod
    def from_request(cls, request, **kwargs):
        api = kwargs.pop("api")
        start = TimepointConstraint(earliest_time=request.earliest_start_time,
                                    latest_time=request.latest_start_time)
        temporal = TemporalConstraints(start=start,
                                       duration=Duration())
        constraints = TaskConstraints(hard=request.hard_constraints, temporal=temporal)
        task = cls.create_new(request=request, constraints=constraints)
        if api:
            task.api = api
            task.publish_task_update()
        return task


class TaskProgress(EmbeddedMongoModel):

    current_action = fields.ReferenceField(Action)
    actions = fields.EmbeddedDocumentListField(ActionProgress)

    class Meta:
        ignore_unknown_fields = True

    def update(self, action_id, action_status, **kwargs):
        if action_status == ActionStatus.COMPLETED:
            self.current_action = self._get_next_action(action_id).action.action_id \
                if self._get_next_action(action_id) is not None else self.current_action

        self.update_action_progress(action_id, action_status, **kwargs)

    def update_action_progress(self, action_id, action_status, **kwargs):
        idx = self._get_action_index(action_id)
        action_progress = self.actions.pop(idx)
        action_progress.status = action_status
        if kwargs.get("start_time"):
            action_progress.start_time = kwargs.get("start_time")
        if kwargs.get("finish_time"):
            action_progress.finish_time = kwargs.get("finish_time")
        self.actions.insert(idx, action_progress)

    def complete(self):
        self.current_action = None
        self.save(cascade=True)

    def get_action(self, action_id):
        idx = self._get_action_index(action_id)
        return self.actions[idx]

    def _get_action_index(self, action_id):
        if isinstance(action_id, str):
            action_id_ = uuid.UUID(action_id)
        else:
            action_id_ = action_id

        idx = None
        for a in self.actions:
            if a.action.action_id == action_id_:
                idx = self.actions.index(a)

        return idx

    def _get_next_action(self, action_id):
        idx = self._get_action_index(action_id)
        try:
            return self.actions[idx + 1]
        except IndexError:
            # The last action has no next action
            return None

    def initialize(self, action_id, task_plan):
        self.current_action = action_id
        for action in task_plan[0].actions:
            self.actions.append(ActionProgress(action.action_id))


class TaskStatus(MongoModel):
    task = fields.ReferenceField(Task, primary_key=True, required=True)
    status = fields.IntegerField(default=TaskStatusConst.UNALLOCATED)
    delayed = fields.BooleanField(default=False)
    progress = fields.EmbeddedDocumentField(TaskProgress)

    objects = TaskStatusManager()

    class Meta:
        archive_collection = 'task_status_archive'
        ignore_unknown_fields = True

    def archive(self):
        with switch_collection(TaskStatus, TaskStatus.Meta.archive_collection):
            super().save()
        self.delete()

    def update_progress(self, action_id, action_status, **kwargs):
        self.refresh_from_db()
        if not self.progress:
            self.progress = TaskProgress()
            self.progress.initialize(action_id, self.task.plan)
            self.save()
        self.progress.update(action_id, action_status, **kwargs)
        self.save(cascade=True)
