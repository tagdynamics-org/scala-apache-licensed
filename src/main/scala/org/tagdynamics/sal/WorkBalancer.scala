package org.tagdynamics.sal

/**
 * The `balancer` method below is copied from the Akka documentation, licensed under the
 * Apache 2.0 license.
 *
 * See:
 *  - https://github.com/akka/akka/blob/master/akka-docs/src/test/scala/docs/stream/cookbook/RecipeWorkerPool.scala
 *  - https://doc.akka.io/docs/akka/2.5/stream/stream-cookbook.html
 *  - https://blog.colinbreck.com/partitioning-akka-streams-to-maximize-throughput/
 *
 */

object WorkBalancer {

  //  --- start of copy pasted code (from first link above) ---

  import akka.NotUsed
  import akka.stream.FlowShape
  import akka.stream.scaladsl._

  def balancer[In, Out](worker: Flow[In, Out, Any], workerCount: Int): Flow[In, Out, NotUsed] = {
    import GraphDSL.Implicits._

    Flow.fromGraph(GraphDSL.create() { implicit b ⇒
      val balancer = b.add(Balance[In](workerCount, waitForAllDownstreams = true))
      val merge = b.add(Merge[Out](workerCount))

      for (_ ← 1 to workerCount) {
        // for each worker, add an edge from the balancer to the worker, then wire
        // it to the merge element
        balancer ~> worker.async ~> merge
      }

      FlowShape(balancer.in, merge.out)
    })
  }

  //  --- end of copy pasted code ---

}
