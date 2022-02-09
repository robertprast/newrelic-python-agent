# Copyright 2010 New Relic, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import collections
import threading

try:
    from newrelic.core.infinite_tracing_pb2 import AttributeValue
except:
    AttributeValue = None


import logging
logger = logging.getLogger(__name__)

class StreamBuffer(object):

    def __init__(self, maxlen):
        self._queue = collections.deque(maxlen=maxlen)
        self._notify = self.condition()
        self._shutdown = False
        self._disconnect = False
        self._seen = 0
        self._dropped = 0
        self._stream = None

    @staticmethod
    def condition(*args, **kwargs):
        return threading.Condition(*args, **kwargs)

    def shutdown(self):
        with self._notify:
            self._shutdown = True
            self._notify.notify_all()

    def disconnect(self):
        with self._notify:
            self._disconnect = True
            self._stream = None
            self._notify.notify_all()

    def reconnect(self):
        with self._notify:
            self._disconnect = False

    def put(self, item):
        with self._notify:
            if self._shutdown:
                return

            self._seen += 1

            # NOTE: dropped can be over-counted as the queue approaches
            # capacity while data is still being transmitted.
            #
            # This is because the length of the queue can be changing as it's
            # being measured.
            if len(self._queue) >= self._queue.maxlen:
                self._dropped += 1

            self._queue.append(item)
            self._notify.notify_all()

    def stats(self):
        with self._notify:
            seen, dropped = self._seen, self._dropped
            self._seen, self._dropped = 0, 0

        return seen, dropped

    def __next__(self):
        while True:
            with self._notify:
                if self._shutdown or self._disconnect or (self._stream and self._stream.code() is not None):
                    # When a gRPC stream receives a server side disconnect (usually in the form of an OK code)
                    # the item it is waiting to consume from the iterator will not be sent, and
                    # will inevitably be lost.

                    # StopIteration raised when the StreamBuffer has been shutdown, during a reconnect,
                    # or when the stream is about to close and will lose the next item in the queue
                    logger.debug("Stream buffer disconnected or shutdown. Refusing to iterate.")
                    raise StopIteration

                try:
                    return self._queue.popleft()
                except IndexError:
                    pass

                if not self._shutdown and not self._disconnect and not self._queue:
                    self._notify.wait()

    next = __next__

    def __iter__(self):
        return self


class SpanProtoAttrs(dict):
    def __init__(self, *args, **kwargs):
        super(SpanProtoAttrs, self).__init__()
        if args:
            arg = args[0]
            if len(args) > 1:
                raise TypeError(
                        "SpanProtoAttrs expected at most 1 argument, got %d",
                        len(args))
            elif hasattr(arg, 'keys'):
                for k in arg:
                    self[k] = arg[k]
            else:
                for k, v in arg:
                    self[k] = v

        for k in kwargs:
            self[k] = kwargs[k]

    def __setitem__(self, key, value):
        super(SpanProtoAttrs, self).__setitem__(key,
                SpanProtoAttrs.get_attribute_value(value))

    def copy(self):
        copy = SpanProtoAttrs()
        copy.update(self)
        return copy

    @staticmethod
    def get_attribute_value(value):
        if isinstance(value, bool):
            return AttributeValue(bool_value=value)
        elif isinstance(value, float):
            return AttributeValue(double_value=value)
        elif isinstance(value, int):
            return AttributeValue(int_value=value)
        else:
            return AttributeValue(string_value=str(value))
