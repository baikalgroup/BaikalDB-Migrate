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

import sys
import os
import threading

class BinlogSender(threading.Thread):
    def __init__(self, config, queue, callbak):
        threading.Thread.__init__(self)
        self.queue = queue
        self.callbak = callbak
        pass
        #raise NotImplementedError
    def send(self,binlogItemList):
        pass
        #raise NotImplementedError
    def init(self):
        pass
        #raise NotImplementedError
    def run(self):
        while True:
            item = self.queue.get()
            ts = item['commitTs']
            self.callbak(ts)
            print item
