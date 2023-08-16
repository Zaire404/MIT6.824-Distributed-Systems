## MapReduce 是如何实现的

## Some Questions
1、Can map or reduce run twice? 
可以，如果一个worker因为网络原因被coordinator(master)判断为crashed，则会将任务分配给另一个worker，但是两个worker都完成了任务。

2、worker在向coordinator询问任务后被赋予map/reduce任务，那么worker怎么判断给什么任务最有效？

3、接第二个问题，有或者说worker做什么任务是预先确定的。按论文中所述，做map任务的worker在load file阶段是不需要网络传输的，所以做map任务的worker是提前就知道是哪些的，而剩下远程的都是做reduce任务的worker。


基本上一台机器运行一个map函数或一个reduce函数，而不是多个。

当工作接近尾声时，coordinator将剩余的任务分配给另外的机器，防止有机器落后的情况，然后他们会取先完成的结果。
Exactly the slower workers are called straggler and what they do is like they sort of do backup tasks.This is a common idea to deal with stragglers to deal with tail latency, is to try to basically replicate tasks and go for the first that finishes.

