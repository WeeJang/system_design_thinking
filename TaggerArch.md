# 设计目标
 
  这是一个简单的打tag流程服务。

  涉及到几个节点：
  
  1. 上游源将json消息推送到kafka中的topic1;

  2. 我方解析消息，将数据进行打tag，期间可能需要调取spider接口获取网页数据，

     然后将打完tag的数据推送到kafka中的topic2;

  3. 注意过程中可能失败，需要加监控；

  4. 注意spider接口是一个network-io的耗时操作；

  5. 打标是一个cpu的耗时操作;


# 系统设计

## 开发栈
   python(2.7)
   pykakfa
   gevent

## 思考

系统也是经过了几次迭代。系统中有一个关键：可能需要调用spider进行网页抓取，这是个耗时NetworkIO; 

还需要监控组件,这个也需要访问邮件网关,所以选用的gevent;

同时,各个部分组件（消息监听／消息分类／网页抓取／结果推送／监控褒奖）之间使用Queue进行解耦
   
## code demo

```python

#-*- coding:utf-8 -*-
from gevent import monkey; monkey.patch_socket()
import time,datetime
import json
import argparse
import ConfigParser
import traceback

import logging
from logging import Formatter
from logging.handlers import TimedRotatingFileHandler

import requests
import socket
import gevent
#from gevent.coros import BoundedSemaphore
from gevent.queue import Queue

from pykafka import KafkaClient
from pykafka.common import OffsetType

from MLTagger import MLTagger
import WebPager
#from WebPager import get_web_from_service,get_pure_text_from_html

CUR_DICT_PATH = os.path.split(os.path.realpath(__file__))[0]
_CONF_PATH = os.path.join(CUR_DICT_PATH,"../../conf")

#{{{ logging
LOG_FILE_PATH = os.path.join(CUR_DICT_PATH,"../../log/tagger_server.log")
LOGRECORD_FORMAT="%(thread)d %(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s"
LOGRECORD_DATEFMT="%a, %d %b %Y %H:%M:%S"

server_logger_formatter = Formatter(LOGRECORD_FORMAT,LOGRECORD_DATEFMT)
server_logger_handler = TimedRotatingFileHandler(filename=LOG_FILE_PATH,when="D")
server_logger_handler.setFormatter(server_logger_formatter)

logger  = logging.getLogger(__name__)
logger.setLevel("INFO")
logger.propagate = 0
logger.addHandler(server_logger_handler)
#logging }}}

##{{{ Service
WEB_GETTER_NUM = 4
SERVICE_TYPE = "wx"
MAIL_API = "" #Monitor-Mail Gateway
SPIDER_API = u"" #Spider API
SPIDER_TIMEOUT = 30

msg_without_html_queue = Queue()  #原始json消息
msg_with_html_queue = Queue()  #添加抓取网页信息的消息
result_queue = Queue() #打上结果后的消息
failed_msg_queue = Queue() #网页抓取超时需要发报警邮件的消息
##Service }}}

_kafka_host,_kafka_sub_topics,_kafka_pub_topics,_kafka_consumer_group,\
        _monitor_mail_list,_monitor_mail_token,_monitor_mail_sub \
    = None,None,None,None,\
       None,None,None

def parse_conf_file(conf_path,section = SERVICE_TYPE):
    """ 解析配置文件 """
    global _kafka_host,_kafka_sub_topics,_kafka_pub_topics,_kafka_consumer_group,\
           _monitor_mail_list,_monitor_mail_token,_monitor_mail_sub

    conf_parser = ConfigParser.ConfigParser()
    conf_parser.read(conf_path)
    _kafka_host = conf_parser.get(section,"host")
    _kafka_sub_topics = conf_parser.get(section,"sub_topics")
    _kafka_pub_topics = conf_parser.get(section,"pub_topics")
    _kafka_consumer_group = conf_parser.get(section,"consumer_group")

    _monitor_mail_list = conf_parser.get(section,"monitor_mail_list")
    _monitor_mail_token = conf_parser.get(section,"monitor_mail_token")
    _monitor_mail_sub = conf_parser.get(section,"monitor_mail_sub")

class KafkaListener(object):
    """
    监听推送过来的Kafka数据
    """
    def __init__(self,name,kafka_host,kafka_sub_topics,kafka_consumer_group):
        self.__name__ = name
        self.__host__,self.__sub_topics__,self.__consumer_group \
                = kafka_host,kafka_sub_topics,kafka_consumer_group
        self.__kafka_client__ = KafkaClient(self.__host__,use_greenlets=True)
        self.__subtopics__ = self.__kafka_client__.topics[self.__sub_topics__]
        self.__consumer__ = self.__subtopics__.get_simple_consumer(\
                                   consumer_group=self.__consumer_group,\
                                    #auto_offset_reset=OffsetType.EARLIEST,\
                                    #reset_offset_on_start=True,\
                                    )
        self.__running__ = False

    def start(self):
        global msg_without_html_queue
        logger.info("[%s] run start" % (self.__name__))
        self.__running__ = True
        while self.__running__:
            try:
                msg = self.__consumer__.consume(block=False)
                if msg is not None:
                    raw_json = json.loads(msg.value)
                    logger.info("[%s] msg offset is : %s ,text is : %s" % (self.__name__,msg.offset,raw_json))
                    msg_without_html_queue.put(raw_json)
                    self.__consumer__.commit_offsets()
                gevent.sleep(0)
            except Exception as exc:
                logger.error("[%s] run exception : %s" % (self.__name__,exc))
        logger.info("[%s] run end" % (self.__name__))

    def stop(self):
        if self.__running__:
            self.__running__ = False


class WebGetter(object):
    """ 
    负责进行网页抓取的组件
    注意超时的设置
    """
    def __init__(self,name):
        self.__name__ = name
        self.__running__ = False

    def insert_web_info_to_json(self,raw_json):
        assert isinstance(raw_json,dict)
        type_ = raw_json.get("type",None)
        url  = raw_json.get("url",None)
        if type_ == 3 and \
                url != u"":
            #full_url = "url=%s" % (url.encode("utf-8"))
            full_url = url
            raw_html,pure_text = u"",u""
            web_get_elapsed = 0
            try:
                with gevent.Timeout(SPIDER_TIMEOUT,False):
                    logger.error("[%s] get web :[%s]..."  % (self.__name__,full_url.encode("utf-8")))
                    #raw_html = WebPager.get_web_from_service(query =full_url)
                    start = time.time()
                    logger.error("[%s] get web :[%s]..."  % (self.__name__,full_url.encode("utf-8")))
                    raw_html = WebPager.post_url_value(url_value=full_url,url=SPIDER_API)
                    logger.error("[%s] get web :[%s]..."  % (self.__name__,full_url.encode("utf-8")))
                    web_get_elapsed = time.time() - start
                    logger.error("[%s] get web [%s] SUCCESS, consume [%s]s, rawhtml :[%s]"  % (self.__name__,full_url,web_get_elapsed,type(raw_html)))
                    pure_text = WebPager.get_pure_text_from_html(raw_html)
                    logger.error("[%s] get web :[%s] , pure_text is [%s]"  % (self.__name__,full_url,type(pure_text)))
            except:
                logger.error(traceback.format_exc())

            if pure_text == u"":
                logger.error("[%s] get web :[%s] Timeout"  % (self.__name__,full_url.encode("utf-8")))
                failed_json_msg = {"Exception":"Timeout","raw_json":raw_json}
                failed_msg_queue.put(failed_json_msg)
            raw_json["webCrawlConsume"] = web_get_elapsed
            raw_json["content"] = pure_text
            raw_json["contentHtml"] = raw_html

    def start(self):
        global msg_without_html_queue
        global msg_with_html_queue
        logger.info("[%s] run start" % (self.__name__))
        self.__running__ = True
        while self.__running__:
            try:
                if not msg_without_html_queue.empty():
                    msg = msg_without_html_queue.get()
                    logger.info("[%s] get msg %s " % (self.__name__,msg["text"].encode("utf-8")))
                    #web_get = gevent.spawn(self.insert_web_info_to_json,msg)
                    #web_get.join()
                    self.insert_web_info_to_json(msg)
                    msg_with_html_queue.put(msg)
                gevent.sleep(0)
            except Exception as exc:
                logger.error("[%s] run exception : %s" % (self.__name__,exc))
        logger.info("[%s] run end" % (self.__name__))

    def stop(self):
        if self.__running__:
            self.__running__ = False

class Tagger(object):
    """ 
      消息打标
    """
    def __init__(self,name,conf_path=_CONF_PATH,service_type=SERVICE_TYPE):
        self.__name__ = name
        self.__running__ = False
        self.__tagger__ = MLTagger(_CONF_PATH,service_type)

    def start(self):
        global msg_with_html_queue
        global result_queue
        logger.info("[%s] run start" % (self.__name__))
        self.__running__ = True
        while self.__running__:
            try:
                if not msg_with_html_queue.empty():
                    msg = msg_with_html_queue.get()
                    logger.info("[%s] get msg %s " % (self.__name__,msg["text"].encode("utf-8")))
                    self.__tagger__.tag(msg)
                    result_queue.put(msg)
                gevent.sleep(0)
            except Exception as exc:
                logger.error("[%s] run exception : %s" % (self.__name__,exc))
        logger.info("[%s] run end" % (self.__name__))

    def stop(self):
        if self.__running__:
            self.__running__ = False


class ResultSender(object):
    """
    结果推送回kafka 
    """
    def __init__(self,name,kafka_host,kafka_pub_topics):
        self.__name__ = name
        self.__host__,self.__pub_topics__ \
                = kafka_host,kafka_pub_topics
        self.__kafka_client__ = KafkaClient(self.__host__,use_greenlets=True)
        self.__pubtopics__ = self.__kafka_client__.topics[self.__pub_topics__]
        self.__producer__ =  self.__pubtopics__.get_producer(sync=True)
        self.__running__ = False

    def start(self):
        global result_queue
        logger.info("[%s] run start" % (self.__name__))
        self.__running__ = True
        while self.__running__:
            try:
                if not result_queue.empty():
                    msg = result_queue.get()
                    msg_str = json.dumps(msg)
                    logger.info("[%s] __tagger_result__:%s" % (self.__name__,msg_str,))
                    self.__producer__.produce(msg_str)
                gevent.sleep(0)
            except Exception as exc:
                logger.error("[%s] run exception : %s" % (self.__name__,exc))
        logger.info("[%s] run end" % (self.__name__))

    def stop(self):
        if self.__running__:
            self.__running__ = False


class MailSender(object):
    """
       监控邮件发送 
    """
    def __init__(self,name,mail_list_str,mail_token,mail_sub):
        self.__name__ = name
        self.__mail_list_str__ = mail_list_str
        self.__mail_token__ = mail_token
        self.__mail_sub__ = mail_sub
        self.__running__ = False

    def send_simple_message(self,content,mail_api=MAIL_API):
        data={"token": self.__mail_token__,\
                "to": self.__mail_list_str__,
                "subject": self.__mail_sub__,
                "content": content}
        WebPager.post_field_dict(data,mail_api)

    def start(self):
        global failed_msg_queue
        logger.info("[%s] run start" % (self.__name__))
        self.__running__ = True
        while self.__running__:
            try:
                if not failed_msg_queue.empty():
                    msg = failed_msg_queue.get()
                    msg_str = json.dumps(msg)
                    logger.info("[%s] Failed MSG __tagger_result__:%s" % (self.__name__,msg_str,))
                    self.send_simple_message(content=msg_str)
                gevent.sleep(60)
            except Exception as exc:
                logger.error("[%s] run exception : %s" % (self.__name__,exc))
        logger.info("[%s] run end" % (self.__name__))

    def stop(self):
        if self.__running__:
            self.__running__ = False

if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(description="Tagger the WeChat-Message")
    arg_parser.add_argument("service_env",help="service_env : [online/offline]")
    args = arg_parser.parse_args()
    service_env = args.service_env
    if service_env is None:
        raise Exception("Must specify the service_env,`online` or `offline`")
    if service_env not in ("online","offline"):
        raise Exception("Illegal service_env parameters,service_env must be `online` or `offline`")
    conf_name = "%s.conf" % (service_env)
    conf_path = os.path.join(_CONF_PATH,conf_name)
    logger.error("conf_path : %s" % (conf_path))

    parse_conf_file(conf_path)

    thread_list = []
    for i in range(WEB_GETTER_NUM):
        name = "WebGetter_%s" % (i)
        thread_list.append(gevent.spawn(WebGetter(name).start))
    thread_list.append(gevent.spawn(\
            KafkaListener("KafkaListener",kafka_host = _kafka_host,\
                     kafka_sub_topics = _kafka_sub_topics,\
                     kafka_consumer_group = _kafka_consumer_group).start)\
            )
    thread_list.append(gevent.spawn(Tagger("Tagger").start))
    thread_list.append(gevent.spawn(\
              ResultSender("ResultSender",kafka_host = _kafka_host,\
                     kafka_pub_topics = _kafka_pub_topics).start)\
              )
    thread_list.append(gevent.spawn(\
              MailSender("MailSender",\
                     mail_list_str = _monitor_mail_list,\
                     mail_token = _monitor_mail_token,mail_sub = _monitor_mail_sub).start)\
              )
    gevent.joinall(thread_list)


``` 
