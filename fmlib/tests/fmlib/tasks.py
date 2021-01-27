from fmlib.db.mongo import MongoStore
from fmlib.models.tasks import Task
from fmlib.models.requests import TaskRequest
from fmlib.models.tasks import TaskStatus
from pymodm.errors import DoesNotExist
from pymodm.errors import ValidationError

if __name__ == '__main__':
    ccu_store = MongoStore(db_name="ccu_store")

    task = Task.get_task_by_request("8f452612-e678-41f7-8321-e0770258575e")
    print("task: ", task.task_id)

    tasks = task.get_parent_tasks()
    print("tasks: ", tasks)


    # task = Task.create_new()
    # print("task: ", task.task_id)
    #
    # # Task is in "task" collection
    # print("task status: ", task.status.status)
    # print("task status: ", Task.get_task_status(task.task_id).status)
    #
    # task.update_status(6)
    #
    # tasks = Task.get_tasks_by_status(6)
    # print("tasks by status: ", tasks)
    #
    # tasks = Task.get_tasks_by_robot(1)
    # print("tasks by robot: ", tasks)
    #
    # tasks = Task.get_tasks()
    # print("all (not archived) tasks: ", tasks)
    #
    # tasks = Task.get_tasks(robot_id=1)
    # print("tasks by robot:", tasks)
    #
    # tasks = Task.get_tasks(status=12)
    # print("tasks by status:", tasks)
    #
    # # tasks = Task.get_tasks(recurrent=True)
    # # print("recurrent tasks: ", tasks)
    #
    # # Task is in "task_archive" collection
    # task.update_status(9)
    # task = Task.get_archived_task(task.task_id)
    # print("task: ", task.task_id)
    # print("task status: ", task.status.status)
    # print("task status: ", Task.get_task_status(task.task_id).status)
    #
    # print("tasks by status: ", tasks)
    #
    # tasks = Task.get_tasks()
    # print("all (not archived) tasks: ", tasks)
    #
