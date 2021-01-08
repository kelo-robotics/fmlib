from pymodm import MongoModel, fields


class User(MongoModel):

    user_id = fields.CharField(primary_key=True)

    @classmethod
    def create_new(cls, **kwargs):
        user = cls(**kwargs)
        user.save()
        return user
