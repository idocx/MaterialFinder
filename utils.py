import json

element_names = tuple(map(
    lambda entry: entry["name"].lower(),
    json.load(open(r"rsc/pt.json", "r", encoding="utf-8"))
))

group_names = tuple(
    json.load(open(r"rsc/common_groups.json", "r", encoding="utf-8"))
)

common_groups = {k: True for k in element_names + group_names}


def prettify(result):
    return json.dumps(result, indent=2)


def make_action(method, index, source):
    _id = source.pop("uid")
    return {
        "_op_type": method,
        "_index": index,
        "_id": _id,
        "_source": source
    }


class Accumulater:
    def __init__(self, init_id=None, callback=None):
        self.init_id = init_id
        self._id = init_id
        self._buffer = set()
        self.callback = callback

    def append(self, input_id, value):
        if self._id == self.init_id:
            self._id = input_id

        if input_id == self._id:
            self._buffer.add(value)
        else:
            if input_id != self.init_id:
                self.flush()
            self._id = input_id

    def flush(self):
        if self.callback is not None and self._buffer:
            self.callback(self._id, list(self._buffer))
            self._buffer = set()

    def close(self):
        self.flush()


class BulkHelper:
    def __init__(self, max_length=10000, callback=None):
        self.max_length = max_length
        self.callback = callback
        self._buffer = []

    def append(self, e):
        self._buffer.append(e)
        if len(self._buffer) >= self.max_length:
            self.flush()

    def flush(self):
        if self.callback is not None and self._buffer:
            self.callback(self._buffer)
        self._buffer.clear()

    def close(self):
        self.flush()
