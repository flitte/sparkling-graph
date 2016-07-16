package ml.sparkling.graph.operators.measures.vertex.katz

import ml.sparkling.graph.api.operators.measures.VertexMeasureConfiguration
import ml.sparkling.graph.operators.MeasureTest
import ml.sparkling.graph.operators.OperatorsDSL._
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Graph

/**
  * Created by Roman Bartusiak (roman.bartusiak@pwr.edu.pl http://riomus.github.io).
  */
class KatzCentrality$Test (implicit sc: SparkContext) extends MeasureTest {


  "Eigenvector  for line graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes eigenvector")
    val result = KatzCentrality.compute(graph)
    Then("Should calculate eigenvector correctly")
    result.vertices.collect().sortBy { case (vId, data) => vId }.map { case (vId, data) => data }.zip(Array(
      0d, 0d, 0d, 0d, 0d
    )).foreach { case (a, b) => {
      a should be(b +- 1e-5)
    }
    }
  }

  "Eigenvector  for line graph" should "be correctly calculated using DSL" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/5_nodes_directed")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes eigenvector")
    val result = graph.katzCentrality()
    Then("Should calculate eigenvector correctly")
    result.vertices.collect().sortBy { case (vId, data) => vId }.map { case (vId, data) => data }.zip(Array(
      0d, 0d, 0d, 0d, 0d
    )).foreach { case (a, b) => {
      a should be(b +- 1e-5)
    }
    }
  }

  "Eigenvector  for full 4 node directed graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes eigenvector")
    val result = KatzCentrality.compute(graph)
    Then("Should calculate eigenvector correctly")
    result.vertices.collect().sortBy { case (vId, data) => vId }.map { case (vId, data) => data }.zip(Array(
      0.32128186442503776, 0.5515795539542094, 0.6256715148839718, 0.44841176915201825
    )).foreach { case (a, b) => {
      a should be(b +- 1e-5)
    }
    }
  }

  "Eigenvector  for full 4 node undirected graph" should "be correctly calculated" in {
    Given("graph")
    val filePath = getClass.getResource("/graphs/4_nodes_full")
    val graph: Graph[Int, Int] = loadGraph(filePath.toString)
    When("Computes eigenvector")
    val result = KatzCentrality.compute(graph, VertexMeasureConfiguration[Int, Int](true))
    Then("Should calculate eigenvector correctly")
    result.vertices.collect().sortBy { case (vId, data) => vId } should equal(Array(
      (1, 0.5), (2, 0.5), (3, 0.5), (4, 0.5)
    ))
  }


}