## MapReduce 是如何实现的

## Some Questions
1、Can map or reduce run twice?
可以，如果一个worker因为网络原因被coordinator(master)判断为crashed，则会将任务分配给另一个worker，但是两个worker都完成了任务。



基本上一台机器运行一个map函数或一个reduce函数，而不是多个。

当工作接近尾声时，coordinator将剩余的任务分配给另外的机器，防止有机器落后的情况，然后他们会取先完成的结果。
Exactly the slower workers are called straggler and what they do is like they sort of do backup tasks.This is a common idea to deal with stragglers to deal with tail latency, is to try to basically replicate tasks and go for the first that finishes.