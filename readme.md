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