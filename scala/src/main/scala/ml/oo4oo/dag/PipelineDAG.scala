package ml.oo4oo.dag

import ml.combust.bundle.BundleFile
import ml.combust.mleap.runtime.MleapSupport.MleapBundleFileOps
import ml.combust.mleap.runtime.frame.{DefaultLeapFrame, Transformer => MleapTransformer}
import ml.combust.mleap.spark.SimpleSparkSerializer
import ml.oo4oo.utils.{Serialize, Utils}
import org.apache.spark.ml.{Pipeline, PipelineModel, PipelineStage}
import org.apache.spark.sql.{DataFrame, SparkSession}
import resource._

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.reflect.ClassTag
import scala.reflect.io.Directory
import scala.reflect.runtime.universe.typeOf
import scala.reflect.runtime.universe.TypeTag

/*
 * need for: join, union, createTable
 * limit, sortBy, filter, explode()
 * */

case class PipelineDAG(nodes: Map[String, Node], finalNodeName: String) extends Serialize {
  var memory = scala.collection.mutable.Map.empty[String, Any]

  def describe(): Unit = {
    val _mem = scala.collection.mutable.Map.empty[String, String]
    var msg  = "describe()\n"

    def iterate_node(node: Node, offset: Int): Unit = {
      msg += "\t" * offset + s"${node.uid}\n"
      node
        .describe()
        .split("\n")
        .foreach(x => {
          msg += "\t" * offset + s"\t${x}\n"
        })

      node.inputs
        .map(key => {
          val upstreamNode = nodes.get(key).get

          if (upstreamNode.getClass.getName == classOf[PlaceHolderNode].getName) {
            val placeHoderDF = upstreamNode.asInstanceOf[PlaceHolderNode].input
            _mem ++= Map(key -> placeHoderDF)
            msg += "\t" * (offset + 1) + s"${placeHoderDF}\n"
          } else if (!_mem.contains(key)) {
            _mem ++= Map(key -> "cached")
            iterate_node(upstreamNode, offset + 1)
          } else {
            msg += "\t" * (offset + 1) + s"${_mem.get(key)}\n"
          }
        })
    }

    val finalNode = nodes.get(finalNodeName).get
    iterate_node(finalNode, 1)

    Utils.formatToTree(msg.split("\n")).map(println)
  }

  def fit(node: Node, inputs: Map[String, DataFrame], verbose: Int): Unit = {
    val verbose_next = if (verbose > 0) {
      val offset = (verbose - 1)
      println("   " * offset + s"└─ ${node.uid}.fit()")
      node
        .describe()
        .split("\n")
        .foreach(x => {
          val n = x.prefixLength(_ == '\t')
          println("   " * (offset + n) + s"└─ ${x.trim}")
        })

      verbose + 1
    } else {
      verbose
    }

    val inputDF = node.inputs.map(x => {
      val upstreamNode = nodes.get(x).get
      val key          = x

      if (upstreamNode.getClass.getName == classOf[PlaceHolderNode].getName) {
        val placeHoderDF = inputs.get(upstreamNode.asInstanceOf[PlaceHolderNode].input).get
        memory ++= Map(key -> placeHoderDF)
        placeHoderDF
      } else if (!memory.contains(key)) {
        fit(upstreamNode, inputs, verbose_next)
        val upstreamOutput = transform[DataFrame](upstreamNode, inputs, verbose_next)
        memory ++= Map(key -> upstreamOutput)
        upstreamOutput
      } else {
        memory.get(key).asInstanceOf[DataFrame]
      }
    })

    node.fit(inputDF.head, inputDF.last)
  }

  def fit(inputs: Map[String, DataFrame], verbose: Int = 0): Unit = {
    if (verbose > 0) {
      println("dag.fit()")
    }

    memory.clear()
    val finalNode = nodes.get(finalNodeName).get
    fit(finalNode, inputs, verbose = verbose)
  }

  def transform[T: ClassTag](node: Node, inputs: Map[String, T], verbose: Int): T = {
    val verbose_next = if (verbose > 0) {
      val offset = (verbose - 1)
      println("   " * offset + s"└─ ${node.uid}.transform()")
      node
        .describe()
        .split("\n")
        .foreach(x => {
          val n = x.prefixLength(_ == '\t')
          println("   " * (offset + n) + s"└─ ${x.trim}")
        })
      verbose + 1
    } else {
      verbose
    }

    node.inputs.foreach(x => {
      if (!memory.contains(x)) {
        val upstreamNode = nodes.get(x).get
        if (upstreamNode.getClass.getName == classOf[PlaceHolderNode].getName) {
          val placeHoderDF = inputs.get(upstreamNode.asInstanceOf[PlaceHolderNode].input).get
          memory ++= Map(x -> placeHoderDF)
        } else {
          val middleOutput = transform[T](upstreamNode, inputs, verbose_next)
          memory ++= Map(x -> middleOutput)
        }
      }
    })

    val inputData = node.inputs.map(x => {
      if (memory.contains(x)) {
        memory.get(x).get.asInstanceOf[T]
      } else {
        throw new Exception(s"${node.inputs.mkString(",")} is not in ${memory.map(_._1).mkString(",")}")
      }
    })

    node.transform(inputData.head, inputData.last)
  }

  def transform[T: TypeTag](inputs: Map[String, T], verbose: Int = 0): T = {
    if (verbose > 0) {
      println("dag.transform()")
    }

    typeOf[T] match {
      case x if x =:= typeOf[DataFrame]        =>
        sparkTransform(inputs.asInstanceOf[Map[String, DataFrame]], verbose).asInstanceOf[T]
      case x if x =:= typeOf[DefaultLeapFrame] =>
        mleapTransform(inputs.asInstanceOf[Map[String, DefaultLeapFrame]], verbose).asInstanceOf[T]
      case _                                   => throw new Exception("only DataFrame and DefaultLeapFrame are supported here")
    }
  }

  def sparkTransform(inputs: Map[String, DataFrame], verbose: Int = 0): DataFrame = {
    memory.clear()
    val finalNode = nodes.get(finalNodeName).get
    transform[DataFrame](finalNode, inputs, verbose)
  }

  def mleapTransform(inputs: Map[String, DefaultLeapFrame], verbose: Int = 0): DefaultLeapFrame = {
    memory.clear()
    val finalNode = nodes.get(finalNodeName).get
    transform[DefaultLeapFrame](finalNode, inputs, verbose)
  }

  def save(basePath: String): Unit = {
    val directory = new Directory(new File(basePath))
    directory.deleteRecursively()

    val filePath = s"${basePath}/metadata.txt"
    nodes.foreach { case (k, v) =>
      v.save(basePath + "/" + k)
    }

    val meta = NodesMetadata(
      nodes
        .map { case (_, v) =>
          NodeMetadata(v.uid, v.inputs, v.getClass.getName)
        }
        .toArray[NodeMetadata]
    )

    val mm = Utils.toJson(Map("nodes_meta" -> meta.toString(), "final_node" -> finalNodeName))
    Files.write(Paths.get(filePath), mm.getBytes(StandardCharsets.UTF_8))
  }
}

object PipelineDAG {
  def apply(stagesCtx: StagesCtx): PipelineDAG = {
    assert(stagesCtx.stages.size == 0, "the stages size must equal to 0")

    val finalNodeName = stagesCtx.node.uid
    PipelineDAG(stagesCtx.ctx, finalNodeName)
  }

  def load(basePath: String): PipelineDAG = {
    val filePath      = s"${basePath}/metadata.txt"
    val objStr        = Files.readString(Paths.get(filePath), StandardCharsets.UTF_8)
    val restoredMap   = Utils.fromJson(objStr)
    val metaStr       = restoredMap.get("nodes_meta").get.asInstanceOf[String]
    val meta          = Serialize.fromString[NodesMetadata](metaStr).nodes
    val finalNodeName = restoredMap.get("final_node").get.asInstanceOf[String]

    val nodes = meta
      .map(x => {
        val clazz = Class.forName(x.className)
        if (x.className == classOf[BinaryNode].getName) {
          val constructor = clazz.getConstructor(classOf[Seq[String]], classOf[String])
          constructor.newInstance(x.inputs, x.uid).asInstanceOf[BinaryNode]
        } else if (x.className == classOf[UnaryNode].getName) {
          val constructor = clazz.getConstructor(classOf[String], classOf[String])
          constructor.newInstance(x.inputs.head, x.uid).asInstanceOf[UnaryNode]
        } else if (x.className == classOf[PlaceHolderNode].getName) {
          val constructor = clazz.getConstructor(classOf[String], classOf[String])
          constructor.newInstance(x.inputs.head, x.uid).asInstanceOf[PlaceHolderNode]
        } else {
          throw new Exception("not suported type")
        }
      })
      .map(x => x.load(basePath + "/" + x.uid))
      .map(x => (x.uid -> x))
      .toMap

    PipelineDAG(nodes, finalNodeName)
  }
}
