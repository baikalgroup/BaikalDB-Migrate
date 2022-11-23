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

import os
import random
import pymysql
import commands
import ConfigParser
import io
import sys
import time
import subprocess
import fileinput
from datetime import datetime


def get_tables(sourceConf):
    conn = pymysql.connect(**sourceConf)
    sor = conn.cursor()
    sql = 'show tables;'
    sor.execute(sql)
    tblist = []
    for row in sor.fetchall():
	tname = str(row[0])
	tblist.append(tname)
    return tblist

def get_count(sourceConf, tb):
    conn = pymysql.connect(**sourceConf)
    sor = conn.cursor()
    sql = 'select count(*) from %s;' % tb
    sor.execute(sql)
    count = sor.fetchone()[0]
    return count

def main(configfile):
    config = ConfigParser.ConfigParser()
    config.read(configfile)
    sourceConf = {"host":config.get('source','host'),
		"port":int(config.get('source','port')),
		"user":config.get('source','user'),
		"password":config.get('source','password'),
		"database":config.get('source','database')}

    tables = get_tables(sourceConf)
    dbcount = 0
    for tb in tables:
	count = get_count(sourceConf, tb)
	print tb, count
        dbcount += count
    print len(tables), dbcount

if __name__ == "__main__":
    main(sys.argv[1])
