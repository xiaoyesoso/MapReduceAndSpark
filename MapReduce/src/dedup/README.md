# 设计思路
数据去重的最终目标是让原始数据中出现次数超过一次的数据在输出文件中只出现一次。我们自然而然会想到将同一个数据的所有记录都交给一台reduce机器，无论这个数据出现多少次，只要在最终结果中输出一次就可以了。具体就是reduce的输入应该以数据作为key，而对value-list则没有要求。当reduce接收到一个<key，value-list>时就直接将key复制到输出的key中，并将value设置成空值。
在MapReduce流程中，map的输出<key，value>经过shuffle过程聚集成<key，value- list>后会交给reduce。所以从设计好的reduce输入可以反推出map的输出key应为数据，value任意。继续反推，map输出数 据的key为数据，而在这个实例中每个数据代表输入文件中的一行内容，所以map阶段要完成的任务就是在采用Hadoop默认的作业输入方式之后，将 value设置为key，并直接输出（输出中的value任意）。map中的结果经过shuffle过程之后交给reduce。reduce阶段不会管每 个key有多少个value，它直接将输入的key复制为输出的key，并输出就可以了（输出中的value被设置成空了）。