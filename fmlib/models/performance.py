import uuid

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
    n_re_allocation_attempts = fields.IntegerField(default=0)
    allocated = fields.BooleanField(default=False)
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

    def update_allocation(self, round_id, allocation_time):
        if isinstance(round_id, uuid.UUID):
            round_id = str(round_id)
        self.allocation.allocation_time[round_id] = allocation_time
        self.allocation.allocated = True
        self.save()

    def update_bids(self, bid):
        if not self.allocation.bids:
            self.allocation.bids = list()
        self.allocation.bids.append(bid)
        self.save()

    def update_n_re_allocation_attempts(self):
        self.allocation.n_re_allocation_attempts += 1
        self.allocation.allocated = False
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
    def get_bids_by_round(cls, round_id):
        bids = list()
        allocations = [task.allocation for task in cls.objects.all()]
        for allocation in allocations:
            for bid in allocation.bids:
                if bid.round_id == round_id:
                    bids.append(bid)
        return bids
