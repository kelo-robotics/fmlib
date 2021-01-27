from fmlib.db.mongo import MongoStore
from pymodm.errors import DoesNotExist
from pymodm.errors import ValidationError
from fmlib.models.actions import Action, WallFollowing

if __name__ == '__main__':
    ccu_store = MongoStore(db_name="ccu_store")

    actions = Action.get_actions(type="WALL_FOLLOWING")
    for a in actions:
        print("actions: ", a.action_id)
        a.archive()

    # actions = WallFollowing.get_archived_actions(area="c022", velocity=0.3)
    # # print("actions: ", actions)
    # for a in actions:
    #     print("action: ", a.action_id)
    #     duration = a.get_action_duration()
    #     print("duration: ", duration)

    # actions = Action.objects.by_action_type("GO_TO")
    #
    # print("actions: ", [action for action in actions])

    # actions = Action.get_actions(type="STANDSTILL")
    # for a in actions:
    #     print("stand still actions: ", a.action_id)
    #     duration = a.get_action_duration()
    #     print("duration: ", duration)
    #
