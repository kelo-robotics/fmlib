import logging
import uuid
from datetime import datetime, timedelta

import dateutil.parser
import pytz
from bson.codec_options import CodecOptions
from fmlib.models import requests
from fmlib.models.actions import Action, ActionProgress, Duration
from fmlib.models.environment import Position
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
            try:
                task_id = uuid.UUID(task_id)
            except ValueError as e:
                raise e

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
    work_time = fields.EmbeddedDocumentField(Duration)
    travel_time = fields.EmbeddedDocumentField(Duration)

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        document['start'] = TimepointConstraint.from_payload(document.get('start'))
        document['finish'] = TimepointConstraint.from_payload(document.get('finish'))
        document['work_time'] = Duration.from_payload(document.get('work_time'))
        document['travel_time'] = Duration.from_payload(document.get('travel_time'))
        temporal_constraints = cls.from_document(document)
        return temporal_constraints

    def to_dict(self):
        dict_repr = self.to_son().to_dict()
        dict_repr.pop('_cls')
        dict_repr['start'] = self.start.to_dict()
        dict_repr['finish'] = self.start.to_dict()
        dict_repr['work_time'] = self.work_time.to_dict()
        dict_repr['travel_time'] = self.travel_time.to_dict()
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
    type = fields.CharField()
    parent_task_id = fields.UUIDField(blank=True)
    request = fields.EmbeddedDocumentField(requests.TaskRequest)
    assigned_robots = fields.ListField(blank=True)
    plan = fields.EmbeddedDocumentListField(TaskPlan, blank=True)
    constraints = fields.EmbeddedDocumentField(TaskConstraints)
    scheduled_time = fields.EmbeddedDocumentField(TimepointConstraint)
    eligible_robots = fields.ListField(blank=True)
    capable_robots = fields.ListField(blank=True)

    objects = TaskManager()

    class Meta:
        archive_collection = 'task_archive'
        ignore_unknown_fields = True
        meta_model = 'task'
        codec_options = CodecOptions(tz_aware=True, tzinfo=pytz.timezone('utc'))

    def save(self):
        try:
            super().save(cascade=True)
        except ServerSelectionTimeoutError:
            logging.warning('Could not save models to MongoDB')

    def publish_task_update(self):
        if hasattr(self, "api"):
            msg = mf.create_message(self)
            msg.type = "TASK-UPDATE"
            self.api.publish(msg, groups=['ROPOD'])

    @classmethod
    def create_new(cls, **kwargs):
        try:
            api = kwargs.pop("api")
        except KeyError:
            api = None
        if 'task_id' not in kwargs.keys():
            kwargs.update(task_id=uuid.uuid4())
        elif 'constraints' not in kwargs.keys():
            start = TimepointConstraint(earliest_time=TimeStamp().to_datetime(),
                                        latest_time=TimeStamp(delta=timedelta(minutes=1)).to_datetime())
            temporal = TemporalConstraints(start=start, work_time=Duration(), travel_time=Duration())
            kwargs.update(constraints=TaskConstraints(temporal=temporal))
        kwargs.update(scheduled_time=TimepointConstraint())
        task = cls(**kwargs)
        task.save()
        if api:
            task.api = api
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
    def work_time(self):
        return self.constraints.temporal.work_time

    @property
    def travel_time(self):
        return self.constraints.temporal.travel_time

    @property
    def earliest_start_time(self):
        return self.constraints.temporal.start.earliest_time

    @property
    def latest_start_time(self):
        return self.constraints.temporal.start.latest_time

    @travel_time.setter
    def travel_time(self, travel_time):
        self.constraints.temporal.travel_time = travel_time
        self.save()

    def update_work_time(self, mean, variance, save_in_db=True):
        self.work_time.update(mean, variance)
        if save_in_db:
            self.save()

    def update_travel_time(self, mean, variance, save_in_db=True):
        self.travel_time.update(mean, variance)
        if save_in_db:
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
        if status in TaskStatus.archived_status:
            task_status.archive()
            self.archive()
        self.publish_task_update()

    def assign_robots(self, robot_ids, save_in_db=True,):
        self.assigned_robots = robot_ids
        # Assigns the first robot in the list to the plan
        # Does not work for single-task multi-robot
        self.plan[0].robot = robot_ids[0]
        if save_in_db:
            self.save()
            self.update_status(TaskStatusConst.ALLOCATED)

    def unassign_robots(self):
        self.assigned_robots = list()
        self.travel_time = Duration()
        self.plan[0].robot = None
        self.save()
        self.update_status(TaskStatusConst.UNALLOCATED)
        self.publish_task_update()

    def update_plan(self, task_plan):
        # Adds the section of the plan that is independent from the robot,
        # e.g., for transportation tasks, the plan between pickup and delivery
        self.plan.append(task_plan)
        self.update_status(TaskStatusConst.PLANNED)
        self.save()

    def schedule(self, earliest_time, latest_time):
        self.scheduled_time.update(earliest_time, latest_time)
        self.update_status(TaskStatusConst.SCHEDULED)
        self.save()

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

    def get_remaining_actions(self):
        if not self.status.progress:
            return self.plan[0].actions
        else:
            actions = list()
            for action_progress in self.status.progress.actions:
                if action_progress.status != ActionStatus.COMPLETED:
                    actions.append(action_progress.action)
            return actions

    @property
    def meta_model(self):
        return self.Meta.meta_model

    @property
    def status(self):
        return self.get_task_status(self.task_id)

    @classmethod
    def get_task(cls, task_id):
        return cls.objects.get_task(task_id)

    @classmethod
    def get_archived_task(cls, task_id):
        with switch_collection(cls, cls.Meta.archive_collection):
            return cls.objects.get_task(task_id)

    @staticmethod
    def get_task_status(task_id):
        if isinstance(task_id, str):
            task_id = uuid.UUID(task_id)
        try:
            return TaskStatus.objects.get({'_id': task_id})
        except DoesNotExist:
            try:
                with switch_collection(TaskStatus, TaskStatus.Meta.archive_collection):
                    return TaskStatus.objects.get({'_id': task_id})
            except DoesNotExist:
                raise

    @staticmethod
    def get_tasks_by_status(status):
        tasks_by_status = [status.task for status in TaskStatus.objects.by_status(status)]
        with switch_collection(TaskStatus, TaskStatus.Meta.archive_collection):
            task_ids = [status.to_son().to_dict()['_id'] for status in TaskStatus.objects.by_status(status)]
            with switch_collection(Task, Task.Meta.archive_collection):
                tasks_by_status.extend([Task.get_task(task_id) for task_id in task_ids])
        return tasks_by_status

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

    @classmethod
    def get_tasks_from_request(cls, request_id):
        tasks = list()
        task_requests = requests.TaskRequests.get_request(request_id)
        for request in task_requests.requests:
            tasks.append(cls.get_task(request.task_id))
        return tasks

    def get_parent_tasks(self, tasks):
        if self.request.parent_task_id:
            task = Task.get_task(self.request.parent_task_id)
            tasks.append(task)
            if task.request.parent_task_id:
                task.get_parent_tasks(tasks)
        return tasks

    def update_progress(self, action_id, action_status, robot_pose, **kwargs):
        status = TaskStatus.objects.get({"_id": self.task_id})
        status.update_progress(action_id, action_status, robot_pose, **kwargs)


class TransportationTask(Task):
    request = fields.EmbeddedDocumentField(requests.TransportationRequest)
    capabilities = fields.ListField(default=["navigation", "docking"])

    objects = TaskManager()

    @classmethod
    def from_request(cls, request, api=None):
        pickup = TimepointConstraint(earliest_time=request.earliest_pickup_time.utc_time,
                                     latest_time=request.latest_pickup_time.utc_time)
        temporal = TemporalConstraints(start=pickup,
                                       work_time=Duration(),
                                       travel_time=Duration())
        constraints = TaskConstraints(hard=request.hard_constraints, temporal=temporal)
        task = cls.create_new(type='transportation', request=request, constraints=constraints, api=api)
        return task


class NavigationTask(Task):
    request = fields.EmbeddedDocumentField(requests.NavigationRequest)
    capabilities = fields.ListField(default=["navigation"])

    objects = TaskManager()

    @classmethod
    def from_request(cls, request, api=None):
        arrival = TimepointConstraint(earliest_time=request.earliest_arrival_time.utc_time,
                                      latest_time=request.latest_arrival_time.utc_time)
        temporal = TemporalConstraints(start=arrival,
                                       work_time=Duration(),
                                       travel_time=Duration())
        constraints = TaskConstraints(hard=request.hard_constraints, temporal=temporal)
        task = cls.create_new(type='navigation', request=request, constraints=constraints, api=api)
        return task


class DefaultNavigationTask(NavigationTask):
    """ Return to default waiting location
    """
    @classmethod
    def from_request(cls, request, api=None):
        arrival = TimepointConstraint(earliest_time=request.earliest_arrival_time.utc_time,
                                      latest_time=request.latest_arrival_time.utc_time)
        temporal = TemporalConstraints(start=arrival,
                                       work_time=Duration(),
                                       travel_time=Duration())
        constraints = TaskConstraints(hard=request.hard_constraints, temporal=temporal)
        task = cls.create_new(type='default_navigation', request=request, constraints=constraints, api=api)
        return task


class GuidanceTask(Task):
    request = fields.EmbeddedDocumentField(requests.GuidanceRequest)
    capabilities = fields.ListField(default=["navigation", "guidance"])

    objects = TaskManager()

    @classmethod
    def from_request(cls, request, api=None):
        arrival = TimepointConstraint(earliest_time=request.earliest_arrival_time.utc_time,
                                      latest_time=request.latest_arrival_time.utc_time)
        temporal = TemporalConstraints(start=arrival,
                                       work_time=Duration(),
                                       travel_time=Duration())
        constraints = TaskConstraints(hard=request.hard_constraints, temporal=temporal)
        task = cls.create_new(type='guidance', request=request, constraints=constraints, api=api)
        return task


class DisinfectionTask(Task):
    request = fields.EmbeddedDocumentField(requests.DisinfectionRequest)
    capabilities = fields.ListField(default=["navigation", "uvc-radiation"])

    objects = TaskManager()

    @classmethod
    def from_request(cls, request, api=None):
        start = TimepointConstraint(earliest_time=request.earliest_start_time.utc_time,
                                    latest_time=request.latest_start_time.utc_time)
        temporal = TemporalConstraints(start=start,
                                       work_time=Duration(),
                                       travel_time=Duration())
        constraints = TaskConstraints(hard=request.hard_constraints, temporal=temporal)
        task = cls.create_new(type='disinfection', request=request, constraints=constraints, api=api)
        return task


class TaskProgress(EmbeddedMongoModel):

    current_action = fields.ReferenceField(Action)
    current_pose = fields.EmbeddedDocumentField(Position)
    actions = fields.EmbeddedDocumentListField(ActionProgress)

    class Meta:
        ignore_unknown_fields = True

    def update(self, action_id, action_status, robot_pose, **kwargs):
        self.current_pose = robot_pose
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

    archived_status = [TaskStatusConst.COMPLETED,
                       TaskStatusConst.CANCELED,
                       TaskStatusConst.ABORTED,
                       TaskStatusConst.FAILED]

    in_timetable = [TaskStatusConst.PLANNED,
                    TaskStatusConst.ALLOCATED,
                    TaskStatusConst.SCHEDULED,
                    TaskStatusConst.DISPATCHED,
                    TaskStatusConst.ONGOING]

    class Meta:
        archive_collection = 'task_status_archive'
        ignore_unknown_fields = True

    def archive(self):
        with switch_collection(TaskStatus, TaskStatus.Meta.archive_collection):
            super().save()
        self.delete()

    def update_progress(self, action_id, action_status, robot_pose, **kwargs):
        self.refresh_from_db()
        if not self.progress:
            self.progress = TaskProgress()
            self.progress.initialize(action_id, self.task.plan)
            self.save()
        self.progress.update(action_id, action_status, robot_pose, **kwargs)
        self.save(cascade=True)

