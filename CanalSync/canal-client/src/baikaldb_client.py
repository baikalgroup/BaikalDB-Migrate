'''
 Copyright (c) 2020-present 17usoft.com, Inc. All Rights Reserved.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
'''

#!/usr/local/bin/python3
import time
import logging
import logging.handlers
import datetime
import pymysql
import traceback 

class BaikalDBClient:
    def __init__(self, host = '',user = '',passwd = '',db = '',port = 3306,charset = 'utf8'):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port
        self.charset = charset
        self.conn = None
        self.cursor = None
        self.logger = logging.getLogger('test')
        if not self.reconn():
            raise Exception("connect error")
        

    def connect(self):
        try:
            self.conn = pymysql.Connection(
                host     = self.host,
                user     = self.user,
                password = self.passwd ,
                db       = self.db,
                port     = self.port,
                charset  = self.charset)
            self.cursor = self.conn.cursor()
            return True
        except pymysql.Error as e:
            self.logger.error(e)
            print(e)
            return False    


    def reconn(self, num = 3, stime = 1): 
        number = 0
        status = False
        while not status and number <= num:
            try:
                self.conn.ping() 
                status = True
            except:
                status = self.connect()
                number += 1
                time.sleep(stime)
        return status
    

    def query(self, exesql):
        flag = 0
        retry = 0
        while flag == 0 and retry < 3:
            try:
                self.cursor.execute(exesql)
                self.conn.commit()
                flag = 1
            except pymysql.Error as e: 
                self.reconn()
                self.logger.error(traceback.format_exc())
                self.conn.rollback()
                time.sleep(1)
                retry = retry + 1
        return flag
