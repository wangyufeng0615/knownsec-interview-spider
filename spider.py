#!/usr/bin/env python
# -*- coding:utf-8 -*-
# Wang Yufeng

import sqlite3
import logging
import re
import time
import sys
import requests
import doctest
from threading import Thread
from Queue import Queue
from optparse import OptionParser
from bs4 import BeautifulSoup

ISOTIMEFORMAT = '%Y-%m-%d %X'           # 时间格式
reload(sys)                             # UTF-8支持
sys.setdefaultencoding("utf-8")


class Spider(object):
    def __init__(self, args):
        # testself
        """
        Test the spider itself.

        >>> options.url = 'http://www.sina.com.cn'
        """

        # 初始化参数
        self.url = args.url
        self.depth = args.depth
        self.threads_number = args.thread
        self.keyword = args.keyword
        self.dbfile = args.dbfile

        # 访问过的URL记录
        self.visited_urls = set()

        # 初始化线程池
        self.threadpool = ThreadPool(self.threads_number)

    def run(self):
        if not self.url.startswith('http://'):
            self.url = 'http://' + self.url
        logging.info('URL:' + self.url)
        self.threadpool.add_task(self.get_data, self.url, self.depth)
        self.threadpool.wait_completion()

    def get_data(self, url, depth):
        # 表名和keyword一致
        database = Database(self.dbfile)
        tablename = self.keyword if self.keyword else "NoKeyword"
        database.create(tablename)

        # request的header，模拟浏览器
        header = {
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 \
                        (KHTML, like Gecko) Chrome/44.0.2403.107 Safari/537.36'
        }

        # 避免重复爬URL
        if url in self.visited_urls:
            logging.warning("此链接已经被爬过, %s" % url)
            return
        else:
            logging.info("将要爬: %s" % url)
            self.visited_urls.add(url)

        # 请求页面
        try:
            response = requests.get(url, headers=header, timeout=3)
            returned_data = response.content
        except Exception as e:
            logging.warning("无法爬取:%s, %s" % (url, e))
            return

        # 解析页面数据
        if returned_data:
            soup = BeautifulSoup(returned_data, "lxml")

            # 去掉title两侧的空格和转移符，处理没有title的页面(用当前时间作为文件名)
            try:
                title = soup.title.string.strip().replace('\r', '').replace('\n', '')
            except:
                title = time.strftime(ISOTIMEFORMAT, time.localtime())

            # 去掉title中的非法字符
            rstr = r"[\/\\\:\*\?\"\<\>\|]"  # '/\:*?"<>|'
            try:
                title = re.sub(rstr, "_", title)
            except:
                title = re.sub(rstr, "_", time.strftime(ISOTIMEFORMAT, time.localtime()))

            # 将数据存为文件
            with open("%s.html" % title, "wb") as f:
                f.write(returned_data)

            # 将数据插入数据库
            if self.keyword:
                if self.keyword in returned_data:
                    database.insert(tablename, url, returned_data)
                    logging.info("%s 插入了 %s, 关键词: %s" % (url, tablename, self.keyword))
            else:
                database.insert(tablename, url, returned_data)
                logging.info("%s 的内容添加到了表 %s, 无关键词" % (url, tablename))

            database.close()  # 断开和数据库的连接
            self.visit(soup, depth - 1)  # 继续爬页面中的URL

    # 爬页面中的URL
    def visit(self, soup, depth):
        if depth > 0:
            for willVisitURL in soup.find_all('a'):
                url = willVisitURL.get('href')
                self.threadpool.add_task(self.get_data, url, depth)


class Database(object):
    def __init__(self, dbfile):
        # 链接数据库
        self.connect = sqlite3.connect(dbfile)
        self.cursor = self.connect.cursor()

    def create(self, table):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS %s\
                            (ID INTEGER PRIMARY KEY AUTOINCREMENT, URL TEXT, DATA TEXT)" % table)
        self.connect.commit()

    def insert(self, table, url, data):
        self.cursor.execute("INSERT INTO %s (URL, DATA) VALUES ('url', 'data')" % table)
        self.connect.commit()

    def close(self):
        self.cursor.close()
        self.connect.close()


class ThreadPool(object):
    def __init__(self, threads_number=10):
        self.tasks = Queue()
        for i in xrange(1, threads_number + 1):
            logging.info("初始化线程%d." % i)
            MyThread(self.tasks, i)

    def add_task(self, func, *args, **kwargs):
        self.tasks.put((func, args, kwargs))
        logging.info("已添加任务.")

    def wait_completion(self):
        self.tasks.join()
        logging.info("所有任务已完成")


class MyThread(Thread):
    def __init__(self, tasks, the_number_of_thread):
        Thread.__init__(self)
        self.tasks = tasks
        self.daemon = True
        self.start()
        self.the_number_of_thread = the_number_of_thread  # 线程序号
        logging.info("线程 %d 开始运行" % self.the_number_of_thread)

    def run(self):
        while True:
            func, args, kwargs = self.tasks.get()
            logging.info("线程 %d 在工作中" % self.the_number_of_thread)
            func(*args, **kwargs)
            self.tasks.task_done()


if __name__ == '__main__':
    # 定义命令行参数
    parser = OptionParser()
    parser.add_option("-u", "--url", action="store", type="string",
                      dest="url", default=None,
                      help="Specify the address of spider.")
    parser.add_option("-d", "--depth", action="store", type="int",
                      dest="depth", default="2",
                      help="Specify the depth of spider. Default value is 1.")
    parser.add_option("-f", "--logfile", action="store", type="string",
                      dest="logfile", default="SpiderLogfile.log",
                      help="Specify the logging file of spider.")
    parser.add_option("-l", "--loglevel", action="store", type="int",
                      dest="loglevel", default="4",
                      help="Specify the logging level of spider. 5 is the most detailed.")
    parser.add_option("--keyword", action="store", type="string",
                      dest="keyword", default=None,
                      help="Specify the logging level of spider.")
    parser.add_option("--thread", action="store", type="int",
                      dest="thread", default="10",
                      help="Specify the number of thread.")
    parser.add_option("--dbfile", action="store", type="string",
                      dest="dbfile", default="spider.db",
                      help="Specify the database of spider.")
    parser.add_option("--testself", action="store_true", dest="testself")
    (options, args) = parser.parse_args()

    # 程序自测
    if options.testself:
        doctest.testmod()

    # 设置日志
    loglevel_argument = {1: logging.CRITICAL,
                         2: logging.ERROR,
                         3: logging.WARNING,
                         4: logging.INFO,
                         5: logging.DEBUG}
    logging.basicConfig(level=loglevel_argument[options.loglevel],
                        filename=options.logfile,
                        format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')

    # 让日志同时输出到文件和终端
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    logging.getLogger('').addHandler(console)

    # 运行爬虫
    spider = Spider(options)
    spider.run()
