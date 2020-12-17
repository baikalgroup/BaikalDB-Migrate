# Copyright (c) 2020-present ly.com, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/usr/bin/python
#-*- coding:utf8 -*-

import threading
import Queue
import time


class BinlogMerger(threading.Thread):

    def __init__(self, inQueueDict, outQueue):
        threading.Thread.__init__(self)
        self.inQueueDict = inQueueDict
        self.outQueue = outQueue
        self.priorityQueue = Queue.PriorityQueue()
        self.setDaemon(True)
        self._stop = False
    def run(self):
        for rid,que in self.inQueueDict.items():
            item = que.get()
            ts = item['commitTs']
            self.priorityQueue.put((ts, (item, que)))
        while not self._stop:
            ts, item = self.priorityQueue.get()
            binlog = item[0]
            que = item[1]
            self.outQueue.put(binlog)
            newbinlog = que.get()
            self.priorityQueue.put((newbinlog["commitTs"],(newbinlog, que)))

