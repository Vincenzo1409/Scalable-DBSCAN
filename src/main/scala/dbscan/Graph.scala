package dbscan

import scala.annotation.tailrec

object Graph {

  //Create an empty graph
  def apply[T](): Graph[T] = new Graph(Map[T, Set[T]]())

}

//Unweighted graph with vertexes and edges
class Graph[T] private (nodes: Map[T, Set[T]]) extends Serializable {

  //Add a given vertex v to the graph
  def addVertex(v: T): Graph[T] = {
    nodes.get(v) match {
      case None    => new Graph(nodes + (v -> Set()))
      case Some(_) => this
    }
  }


  //Insert an edge from `from` to `to`
  def insertEdge(from: T, to: T): Graph[T] = {
    nodes.get(from) match {
      case None       => new Graph(nodes + (from -> Set(to)))
      case Some(edge) => new Graph(nodes + (from -> (edge + to)))
    }
  }

  //Insert a vertex from `one` to `another`, and from `another` to `one`
  def connect(one: T, another: T): Graph[T] = {
    insertEdge(one, another).insertEdge(another, one)
  }

  // Find all vertexes that are reachable from `from`
  def getConnected(from: T): Set[T] = {
    getAdjacent(Set(from), Set[T](), Set[T]()) - from
  }

  // optimized tailrec function to find andjacent points
  @tailrec
  private def getAdjacent(tovisit: Set[T], visited: Set[T], adjacent: Set[T]): Set[T] = {

    tovisit.headOption match {
      case Some(current) =>
        nodes.get(current) match {
          case Some(edges) =>
            getAdjacent(edges.diff(visited) ++ tovisit.tail, visited + current, adjacent ++ edges)
          case None => getAdjacent(tovisit.tail, visited, adjacent)
        }
      case None => adjacent
    }

  }

}
