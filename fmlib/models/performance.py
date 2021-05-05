import uuid

from fmlib.models.robot import RobotManager
from fmlib.models.tasks import TaskManager
from pymodm import fields, MongoModel, EmbeddedMongoModel
from pymodm.errors import DoesNotExist


class BidPerformance(EmbeddedMongoModel):
    round_id = fields.UUIDField()
    robot_id = fields.IntegerField()
    insertion_points = fields.ListField()
    computation_time = fields.FloatField()


class Allocation(EmbeddedMongoModel):
    allocation_time = fields.DictField()
    tasks_to_allocate = fields.DictField()
    bids = fields.EmbeddedDocumentListField(BidPerformance)


class Execution(EmbeddedMongoModel):
    delay = fields.FloatField(default=0.0)
    earliness = fields.FloatField(default=0.0)


class TaskPerformance(MongoModel):
    task_id = fields.UUIDField(primary_key=True, required=True)
    allocation = fields.EmbeddedDocumentField(Allocation)
    execution = fields.EmbeddedDocumentField(Execution)

    objects = TaskManager()

    class Meta:
        ignore_unknown_fields = True
        meta_model = "task-performance"

    @property
    def meta_model(self):
        return self.Meta.meta_model

    @classmethod
    def create_new(cls, task_id):
        performance = cls(task_id=task_id, allocation=Allocation(), execution=Execution())
        performance.save()
        return performance

    def update_allocation(self, round_id, allocation_time, tasks_to_allocate):
        if isinstance(round_id, uuid.UUID):
            round_id = str(round_id)
        self.allocation.allocation_time[round_id] = allocation_time
        self.allocation.tasks_to_allocate[round_id] = tasks_to_allocate
        self.save()

    def update_bids(self, bid):
        if not self.allocation.bids:
            self.allocation.bids = list()
        self.allocation.bids.append(bid)
        self.save()

    def update_delay(self, delay):
        self.execution.delay += delay
        self.save()

    def update_earliness(self, earliness):
        self.execution.earliness += earliness
        self.save()

    @classmethod
    def get_task(cls, task_id):
        try:
            return cls.objects.get_task(task_id)
        except DoesNotExist:
            return cls.create_new(task_id)

    @classmethod
    def get_tasks(cls):
        return [task for task in cls.objects.all()]

    @classmethod
    def get_bids_by_round(cls, round_id):
        bids = list()
        allocations = [task.allocation for task in cls.objects.all()]
        for allocation in allocations:
            for bid in allocation.bids:
                if bid.round_id == round_id:
                    bids.append(bid)
        return bids


class RobotPerformance(MongoModel):
    robot_id = fields.IntegerField(primary_key=True)

    objects = RobotManager()

    class Meta:
        ignore_unknown_fields = True
        meta_model = "robot-performance"

    @property
    def meta_model(self):
        return self.Meta.meta_model

    @classmethod
    def create_new(cls, robot_id, **kwargs):
        performance = cls(robot_id=robot_id)
        performance.save()
        return performance

    @classmethod
    def get_robot(cls, robot_id, **kwargs):
        try:
            performance = cls.objects.get_robot(robot_id)
            return performance
        except DoesNotExist:
            return cls.create_new(robot_id, **kwargs)

