package hazardptr

abstract class Node(var value: Int, var next: Option[Node] = None, var prev: Option[Node] = None)
