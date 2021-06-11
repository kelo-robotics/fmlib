import codecs
import pickle

from pymodm import fields, EmbeddedMongoModel


class Timetable(EmbeddedMongoModel):
    data = fields.CharField()

    def get_timetable(self):
        # Convert to object
        try:
            return pickle.loads(codecs.decode(self.data.encode(), "base64"))
        except RuntimeError:
            return None

    @classmethod
    def from_obj(cls, obj):
        data = codecs.encode(pickle.dumps(obj, 2), "base64").decode()
        timetable = cls(data=data)
        return timetable
