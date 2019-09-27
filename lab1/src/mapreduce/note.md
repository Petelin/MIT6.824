# LAB1 笔记

链接: https://pdos.csail.mit.edu/6.824/labs/lab-1.html

## part 1 补全代码
要求: 
>These tasks are carried out by the doMap() function in common_map.go, and the doReduce() function in common_reduce.go respectively.

其实就是按照comment要求补全代码, 先做doMap()

### doMap()
注释的意思就是说我们要调用用户传来的方法然后自己做一个按照key聚合的Partition, 然后按照他给的文件名和k:v格式输出成中间文件以后给reduce用.