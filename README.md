wikipedia-hadoop [![Build Status](https://jenkins.anycook.de/buildStatus/icon?job=wikipedia-hadoop)](https://jenkins.anycook.de/job/wikipedia-hadoop/)
================

Wikipedia Inputformat and some useful Wikipedia Hadoop utils.

## Usage

At first you have to set the ```WikiInputFormat``` as your job InputFormat: 

```Java
job.setInputFormatClass(WikiInputFormat.class);
```

Your Mappers incoming Key and Value need to be from the types ```LongWritable``` and ```WikiRevisionWritable```.

