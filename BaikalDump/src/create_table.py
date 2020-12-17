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

def get_create_table(mysqlSor, tbname):
    sql = 'show create table %s;' % tbname
    mysqlSor.execute(sql)
    res = ''
    for row in mysqlSor.fetchall():
	lines = str(row[1]).split('\n')
	res = ''
	res = ''.join(lines[:-1])
	res += lines[-1].split('AVG_ROW_LENGTH')[0]
    return res



def create_tables(sourceConf, destConf):
    conn = pymysql.connect(**sourceConf)
    sor = conn.cursor()
    sql = 'show tables;'
    sor.execute(sql)
    tblist = []
    for row in sor.fetchall():
	tname = str(row[0])
	tblist.append(tname)
    destConn = pymysql.connect(**destConf)
    destSor = destConn.cursor()
    for tb in tblist:
	createTable = get_create_table(sor, tb)
	try:
	    destSor.execute(createTable)
	except Exception,e:
	    print str(e)
	    continue
	print tb, " created!"

def main(configfile):
    config = ConfigParser.ConfigParser()
    config.read(configfile)
    sourceConf = {"host":config.get('source','host'),
		"port":int(config.get('source','port')),
		"user":config.get('source','user'),
		"password":config.get('source','password'),
		"database":config.get('source','database')}

    destConf = {"host":config.get('dest','host'),
		"port":int(config.get('dest','port')),
		"user":config.get('dest','user'),
		"password":config.get('dest','password'),
		"database":config.get('dest','database')}

    
    create_tables(sourceConf, destConf)

if __name__ == "__main__":
    main(sys.argv[1])
