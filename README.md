# OO4OO

## Overview
OO4OO stands for One Opus for Online and Offline

This repository aim to build a ML pipeline and could serve as online inference and offline transform.

## Running the project in Scala

Install Java 11 and Scala 2.12.15.

To compile the project, run in your terminator:
```sbt clean compile```

To run the sample project which demonstrates the functionality, run in your terminal:
```sbt run```
or
```
sbt "runMain ml.oo4oo.example.OneToOneTransformerExample"
```

## Running the project in Python

```
export PYTHONPATH=.
python oo4oo/pyspark/test_dag.py

```


## As dependency
If you want to use the library as a dependency to another project you can do, though it is not published to central Maven. 

To do so, clone the repository and run `sbt publishLocal`. 
This will publish the project jar to your local ivy repository from which you can then include `` as a dependency in another local project.

Within your other project, you can then import Spark transformers, use them in pipelines, and seralize them into MLEAP bundles using the below:

```

```

See the [example project](src/main/scala/example/OneToOneTransformerExample.scala) for more examples. 

### License
Released under MIT license.
