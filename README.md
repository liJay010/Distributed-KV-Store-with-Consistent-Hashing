# Distributed-KV-Store-with-Consistent-Hashing

> # 基于一致性哈希算法的分布式键值存储系统

### 概述

* 本项目是一个基于一致性哈希算法的分布式键值存储系统。
* 客户端(client))可以向唯一的协调服务器(coord))发送json格式的报文，完成PUT、GET、DELETE、UPDATE四种操作。
* 由唯一的协调服务器(coord))管理多台存储服务器(slave)，支持随时增删存储服务器节点且不会丢失数据。
* 协调服务器与存储服务器间存在心跳机制，自动检测下线节点，并完成数据转移

### 系统设计

> 系统中存在三种节点，分别是客户端(client)、存储服务器(slave)、协调服务器(coord)

#### **A.客户端 Client**

客户端与协调服务器之间建立TCP连接，然后将用户输入的命令发至协调服务器处，由协调服务器处理后，再将处理结果转发至客户端

四种命令格式：

> put:K:V
>
> get:K
>
> delete:K
>
> update:K:V

#### **B.存储服务器 Slave**

存储服务器与协调服务器之间建立TCP连接，并将自己注册在协调服务器维护的哈希环中，负责存储 `<K,V>`键值对。

存储服务器维护两张哈希表：

**I.OWN TABLE**

存储本节点负责的键值对

**II.PREV TABLE**

存储在哈希环上，本节点的前驱节点负责的键值对

#### **C.协调服务器 Coord**

协调服务器负责处理与其建立连接的客户端与存储服务器。

* **当客户端与它建立连接时：**

接收客户端发来的四种请求，求得Key的hash code，向哈希环中顺时针方向的第一个存储服务器转发客户端请求，之后接收存储服务器发来的处理结果，再将其转发到客户端。（注：还需向该存储服务器的后继节点转发客户端请求，修改它的prev_table）

* **当存储服务器与它建立连接时：**

根据存储服务器的IP:PORT求得hash code，加入哈希环中，并开始键值对的转移。

### **数据转移**

有两种情况会发生数据转移：

**case1：当新的存储服务器节点上线时**

根据新节点的IP:PORT求出hash code，求出它的前驱节点，后继节点

**后继节点**

* 清空prev_table，own_table只保留hash code大于新存储服务器节点hash code的键值对，其余键值对移入prev_table中；
* 向它的前驱节点（即新上线的节点）发送prev_table，充当新节点的own_table；
* 向它的后继节点（即新上线节点的后继的后继节点）发送own_table，充当后继节点的prev_table。

**前驱节点**

* 发送own_table作为新上线节点的prev_table。

**case2：当现有存储服务器节点下线时**

求出下线节点的前驱节点、后继节点

**后继节点**

* 将prev_table合并至own_table中；
* 向它的后继节点（即下线节点的后继的后继节点）发送更新后的own_table，充当后继节点prev_table；

**前驱节点**

* 向它的后继的后继节点（即下线节点的后继节点）发送own_table，充当它的后继的后继节点的prev_table;

### 心跳机制

**存储服务器 Slave：**

* 每4s向协调服务器通过UDP连接发送一条心跳报文

**协调服务器 Coord：**

* 每接收一条Slave的心跳报文就在自己维护的MAP中为 `<IP:PORT,COUNT>`的COUNT加一；
* 每30s遍历一次MAP，如果其中某个Slave对应的COUNT等于0，开始数据转移；

### 编译运行

`g++ -g slaveServer.cpp -o slave COMMON_FUNCN.h jsonstring.h -lpthread`

`g++ -g client.cpp -o client COMMON_FUNCN.h jsonstring.h`

`g++ -g Coordination_Server.cpp -o coord COMMON_FUNCN.h COORD_FUNC.h GLOBAL_CS.h  jsonstring.h -lpthread`

启动coord

` ./coord 127.0.0.1 8888`

启动client

` ./client 127.0.0.1 10000`

启动slave1

` ./slave 127.0.0.1 20000`

通过client发送PUT、GET、DELETE、UPDATE请求，查看coord、slave状态

启动slave2

` ./slave 127.0.0.1 20001`

查看coord、slave1、slave2状态，此时发生数据转移

通过client发送PUT、GET、DELETE、UPDATE请求，查看coord、各个slave状态

启动slave3

` ./slave 127.0.0.1 20002`

查看coord、slave1、slave2，slave3状态，此时发生数据转移

关闭slave1，查看coord、slave2、slave3状态，此时也发生了数据转移

### TODO
* 单个协调服务器节点负载太高，容易产生单点故障，可以为其建立协调服务器集群，由哨兵集群负责主从节点切换，提高可用性。
* 目前节点之间通信采用同步方式，可以改为异步方式进行通信，然后采用分布式共识算法(Raft/Paxos)满足数据的一致性。
* 目前采用per client，per thread的简单服务器模型，可改为per client，per eventloop的多Reactor多线程模型。
