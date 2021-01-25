""" Adapted from:
https://realpython.com/inheritance-composition-python/#mixing-features-with-mixin-classes
"""
import uuid
from datetime import datetime

from fmlib.utils.messages import Document


class AsDictMixin:

    def to_dict(self):
        return {
            prop: self._represent(value)
            for prop, value in self.__dict__.items()
            if not self.is_internal(prop)
        }

    @classmethod
    def _represent(cls, value):
        if isinstance(value, object):
            if hasattr(value, 'to_dict'):
                return value.to_dict()
            elif hasattr(value, 'to_str'):
                return value.to_str()
            elif isinstance(value, uuid.UUID):
                return str(value)
            elif isinstance(value, datetime):
                return value.isoformat()
            else:
                return value
        else:
            return value

    @staticmethod
    def is_internal(prop):
        return prop.startswith('_')

    @classmethod
    def from_payload(cls, payload):
        document = Document.from_payload(payload)
        document.pop("metamodel", None)
        return cls.from_dict(document)

    @classmethod
    def from_dict(cls, dict_repr):
        attrs = cls.to_attrs(dict_repr)
        return cls(**attrs)
