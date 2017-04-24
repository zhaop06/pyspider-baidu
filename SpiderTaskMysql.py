# -*- coding: utf-8 -*-
# 2017-3-6 by zhaopeng

import mysql.connector
import datetime

# Connect to the database
class SpiderMysql:
    conn = None
    database_host = "192.168.1.19"
    database_user = "pyspider"
    database_pwd = "viewsonic"
    def __init__(self, db):
        self.conn = mysql.connector.connect(user=self.database_user, password=self.database_pwd,
                                            host=self.database_host, database=db)
        self.cursor = None
        if self.conn is not None:
            self.cursor = self.conn.cursor()


    def select(self, sql_select):
        if self.conn is None:
            return None
        #cursor = self.conn.cursor()
        # Read a single record
        self.cursor.execute(sql_select)
        return self.cursor.fetchone()

    def selectall(self, sql_select):
        if self.conn is None:
            return None
        #cursor = self.conn.cursor()
        # Read a single record
        self.cursor.execute(sql_select)
        return self.cursor.fetchall()

    def insert(self, sql_insert, data):
        #cursor = self.conn.cursor()
        self.cursor.execute(sql_insert, data)
        self.conn.commit()
        return True

    def updata(self, sql_updata, data):
        self.cursor.execute(sql_updata, data)
        self.conn.commit()
        return True

    def close(self):
        if self.conn is not None:
            self.conn.close()


def SelectTask():
    try:
        table = SpiderMysql("spider")
        result = table.select("SELECT * FROM `task` WHERE `status` = 'not start' ORDER BY priority DESC")
        table.close()
        if result is not None:
            table = SpiderMysql("spider")
            if result[3] == 0:
                table.updata("update task set status=%s where id=%s", ("starting", result[0]))
            else:
                table.updata("update task set status=%s ,start_time=%s ,end_time=%s where id=%s", ("starting", datetime.date.today(), datetime.date.today(), result[0]))
            table.close()
        else:
            table = SpiderMysql("spider")
            table.updata("update task set status=%s where lasting=%s", ("not start", 1))
            table.close()
            #result = table.selectall("SELECT * FROM `task` WHERE `lasting` = 1 ORDER BY priority DESC")
            #table.close()
        return result
    except mysql.connector.Error as e:
        print("{}".format(e))
        table.close()
        return None

def InsertUrl(url):
    try:
        table = SpiderMysql("spider")
        result = table.select("SELECT * FROM `url` WHERE `task_id` = %d AND `task_url` LIKE '%s'" % url)
        table.close()
        if result is None:
            table = SpiderMysql("spider")
            sql_insert = "insert into url (task_id, task_url) values (%s, %s)"
            flag = table.insert(sql_insert, url)
            table.close()
        else:
            flag = True

        if flag is True:
            return True
        else:
            return False
    except:
        table.close()
        return False

def InsertResult(result):
    try:
        table = SpiderMysql("spider")
        sql_insert = "insert into result (url, text) values (%s, %s)"
        flag = table.insert(sql_insert, result)
        table.close()
        if flag is True:
            return True
        else:
            return False
    except:
        table.close()
        return False

def SelectSiteTask():
    try:
        table = SpiderMysql("spider")
        result = table.select("SELECT * FROM `site_task` WHERE `status` = 'not start' ORDER BY priority DESC")
        table.close()
        if result is not None:
            table = SpiderMysql("spider")
            table.updata("update site_task set status=%s where id=%s", ("starting", result[0]))
            table.close()
        else:
            table = SpiderMysql("spider")
            table.updata("update site_task set status=%s where %s", ("not start", "1"))
            table.close()
        return result
    except mysql.connector.Error as e:
        print("{}".format(e))
        table.close()
        return None

def InsertSiteUrl(url):
    try:
        table = SpiderMysql("spider")
        result = table.select("SELECT * FROM `site_url` WHERE `task_id` = %d AND `task_url` LIKE '%s'" % url)
        table.close()
        if result is None:
            table = SpiderMysql("spider")
            sql_insert = "insert into site_url (task_id, task_url) values (%s, %s)"
            flag = table.insert(sql_insert, url)
            table.close()
        else:
            flag = True

        if flag is True:
            return True
        else:
            return False
    except:
        table.close()
        return False

def InsertSiteResult(result):
    try:
        table = SpiderMysql("spider")
        sql_insert = "insert into site_result (url, text) values (%s, %s)"
        flag = table.insert(sql_insert, result)
        table.close()
        if flag is True:
            return True
        else:
            return False
    except:
        table.close()
        return False



'''

print(SelectSiteTask())
url = (2, 'http://aaa.aaa.aaa/aaa/aaa/aaaa/aaa')
url1 = (3, 'http://bbb.aaa.aaa/aaa/aaa/aaaa/nnn')
xxx = InsertUrl(url1)
if xxx is True:
    print("insert successed!")
'''
'''
table = SpiderMysql("spider")
result = table.selectall("SELECT DISTINCT `task_id`, `task_url` FROM `url` ")
table.close()
for xx in result:
    print(xx)
'''
'''
table = SpiderMysql("spider")
result = table.selectall("SELECT * FROM `url` WHERE `task_url` LIKE 'http://www.baidu.com/link?url=c3xWz9JrbbFFWQ_yTgqmTkFA2hjPhRHsQc-xpFNa-tI-nbWKVgohJ6jyCqB3XnN3IBqNrKs1CVt-PXEiT6fP-a' ")
table.close()
for xx in result:
    print(xx)
'''
'''
table = SpiderMysql("spider")
result = table.select("SELECT * FROM `url` WHERE `task_id` = %d AND `task_url` LIKE '%s'" % (1, 'http://www.baidu.com/link?url=c3xWz9JrbbFFWQ_yTgqmTkFA2hjPhRHsQc-xpFNa-tI-nbWKVgohJ6jyCqB3XnN3IBqNrKs1CVt-PXEiT6fP-a'))
table.close()
print(result)
'''
'''
result = ('http://adfasdfasdf', '{"content": "bbbbbbbbbbbb", "date": "dadadadadadaad", "title": "aaaaaaaaaaaaaaa", "url": "xxxxxxxxxxxx"}')
InsertResult(result)
'''