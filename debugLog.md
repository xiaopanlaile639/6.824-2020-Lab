# Lab 2A:



## bug1

1、说明：对于Lab2 A实现，如果follower->candidate 和candidate->candidate（again）的time out时间设置的稍微大些，就会出现死锁的现象。

![image-20200623212146325](D:\Typora\data_img\image-20200623212146325.png)

如果设置的比较小，第一个test能通过，但是会出现警告信息：

![image-20200623212416203](D:\Typora\data_img\image-20200623212416203.png)

而且第二个test无法通过。



- 结果：换了一种架构方案。原来的那种算是多线程，也就是在一个raft中开三个独立的goruntine分别管理followe、candidate、还有leader的工作，各个线程的协调会比较复杂。在参考了别人的代码之后，修改了架构思路，采用了单线程轮询的方式分别管理三个状态（从某种方式上，也叫做事件驱动下的多线程）。最终的代码，基本上是ok的。

![image-20200626174548744](D:\Typora\data_img\image-20200626174548744.png)

note：当然，这里说是基本上ok的，因为并没有测试很多次，这里只连续测试5次，全部通过，就算ok。 同时，测试结果汇总没有出现下面的错误。（当然，只限于5次之内）

![image-20200626174706959](D:\Typora\data_img\image-20200626174706959.png)



# Lab2B

## bug2:

说明：如图在进行agreement despite follower disconnection 测试的时候，为什么会出现还未经过选举，就可以直接发送消息了？如下图中的goroutine 2。

![image-20200707113912745](D:\Typora\data_img\image-20200707113912745.png)

结果：问题解决。之所以一直出现数组越界的情况，是因为未考虑到对leader收到follower重复的log成功返回的情况。如下图：

![image-20200707180613517](D:\Typora\data_img\image-20200707180613517.png)

注释的部分是错误的代码，会导致数组越界，因为它不管充不重复，只要成功nextIndex就往后推，这是不正确的。正确的方式应是向下面的那样，每次从args.PrevLogIndex往后推。



# Lab2C

## bug3：

说明：已经提交的日志，会出现回退？？？

![image-20200713183115470](D:\Typora\data_img\image-20200713183115470.png)



结果：解决问题。原因是原来写的appendentries有错误，再追加entries的时候，忘了考虑，leader发来的entires，note结点已经有了，按照原来写的方式就会导致已提交的log发生回退。

修改前的代码：

![image-20200714114207898](D:\Typora\data_img\image-20200714114207898.png)

修改后的代码

![image-20200714114150946](D:\Typora\data_img\image-20200714114150946.png)



## bug4:

说明：lab2c的more persistence 不能通过。 无法在规定的时间内选出leader。

![image-20200714114356045](D:\Typora\data_img\image-20200714114356045.png)

结果：成功调试bug4.不过怎么调的，我具体忘了，现在只知道每次收到response或者request的时候都得进行term的判断。





# Lab3A

## bug5:

说明：无法读到正确的写入kv数据库中的值

![image-20200721113124943](D:\Typora\data_img\image-20200721113124943.png)

![image-20200721113354954](D:\Typora\data_img\image-20200721113354954.png)

2已经不是leader了，但是还能从其中读到值。



## bug6:

说明：client 请求Get的时候，可以从非leader结点得到数据？？

![image-20200721222323693](D:\Typora\data_img\image-20200721222323693.png)

![image-20200721222341981](D:\Typora\data_img\image-20200721222341981.png)

## bug7: 

说明：select 延时似乎不正确，不能产生appInfo,同时服务器端等待的rpc。

![image-20200722113637752](D:\Typora\data_img\image-20200722113637752.png)



## Bug8:

说明： 在参考了别人的设计架构之后，修改自己的代码。最后还是有问题，

![image-20200723143221841](D:\Typora\data_img\image-20200723143221841.png)



结果：lab3a 问题基本解决 bug8主要是因为使用clientid 作为key 来寻找lastcmdInex 不唯一的问题。





# Lab3B：

bug9: 出现死锁问题，原因还未找到。

说明：修改了好多天，死锁问题依然没解决.......



结果1：很奇怪，也没修改什么地方，就莫名其妙的通过了。（当然，也不是每次都能通过）

![image-20200803082537298](D:\Typora\data_img\image-20200803082537298.png)



结果2：后来又出现了死锁问题，敢情原来只是碰巧测试通过。现在发现其实不是死锁，而是程序一直在跑，只是测试无法获得想要的数据，最后发现是Client的 Get函数有问题。如下所示：

修改之前的：

![image-20200809105953826](D:\Typora\data_img\image-20200809105953826.png)

修改之后的：

![image-20200809110334056](D:\Typora\data_img\image-20200809110334056.png)