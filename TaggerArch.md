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

   系统也是经过了几次迭代。系统中有一个关键：

   1)可能需要调用spider进行网页抓取，这是个耗时NetworkIO; 

     还需要监控组件,这个也需要访问邮件网关,所以选用的gevent ;
   
   2)各个部分组件（消息监听／消息分类／网页抓取／结果推送／监控褒奖）之间使用Queue进行解耦
   
## code demo

```python

import python
``` 
