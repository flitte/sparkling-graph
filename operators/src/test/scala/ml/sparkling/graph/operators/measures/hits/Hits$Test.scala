package ml.sparkling.graph.operators.measures.hits

import ml.sparkling.graph.operators.SparkTest
import ml.sparkling.graph.operators.measures.eigenvector.EigenvectorCentrality
import org.apache.spark.graphx.{PartitionStrategy, GraphLoader, Graph}

/**
 * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
 */
class Hits$Test extends SparkTest {

  def appName = "hits-test"


  "Hits  for line graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes Hits")
    val result = Hits.computeBasic(graph)
    Then("Should calculate hits correctly")
    result.vertices.collect().sortBy{case (vId,data)=>vId}.map{case (vId,data)=>data}.zip(Array(
      (0.25,0d), (0.25,0.25),(0.25,0.25),(0.25,0.25),(0d,0.25)
    )).foreach {
      case ((a,b),(c,d)) => {
        a should be (c +- 1e-5)
        b should be (d +- 1e-5)
      }
    }
  }

  "Hits for full 4 node directed graph" should "be correctly calculated" in{
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph:Graph[Int,Int]=loadGraph(filePath.toString)
    When("Computes Hits")
    val result=Hits.computeBasic(graph)
    Then("Should calculate Hits correctly")
    result.vertices.collect().sortBy{case (vId,data)=>vId}.map{case (vId,data)=>data}.zip(Array(
      (0.44504187450168503,0.19806226306818242),
      (0.19806226497496957,0.4450418674109515),
      (1.9336832073590722e-13,0.3568958695205176),
      (0.35689586676523016,3.484376742610991e-13)
    )).foreach {
      case ((a,b),(c,d)) => {
        a should be (c +- 1e-5)
        b should be (d +- 1e-5)
      }
    }
  }



}