## 云计算项目实践期末大作业
### 姓名：袁绍通 学号：17341194
文件结构：
````
├──Inverted
	├── README
	├── input                 输入文件夹    
	│   ├── 2018text1.txt     三篇文章当成输入
	│   ├── 2019text1.txt    
	│   ├── 2020text1.txt    
	├── InvertedIndex.java    实现倒序索引的MapReduce程序
	├── InvertedIndex.class  
	├── 'InvertedIndex$Map.class'
	├── 'InvertedIndex$Combine.class'
	├── 'InvertedIndex$Reducer.class'编译出的相关class文件
	├── InvertedIndex.jar打包后的java程序
````
运行方法：将InvertedIndex.java与input文件夹放到同一个文件夹中，然后
```bash
 javac InvertedIndex.java
 jar -cvf InvertedIndex.jar ./InvertedIndex*.class
  /usr/local/hadoop/bin/hadoop jar InvertedIndex.jar InvertedIndex input output
```
若文件夹下已有output文件夹，则需要先删除再运行，否则会提示输出文件夹已存在。
代码包含main在内的共4个类函数，分别是一个main函数、一个mapper函数和两个Reducer函数。首先进行的是mapper函数，将文本中的符号替换成空格并映射成键值对形式，然后第一个Reducer将单词词频赋给value值，第二个Reducer给key添加文件位置索引，最后的main函数是程序入口，用job的方式运行整个程序。
