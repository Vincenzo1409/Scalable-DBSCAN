package dbscan

case class Rectangle(x: Double, y: Double, x2: Double, y2: Double) {

  /**
   * Returns whether the rectangle is contained by this box
   */
  def contains(other: Rectangle): Boolean = {
    x <= other.x && other.x2 <= x2 && y <= other.y && other.y2 <= y2
  }

  /**
   * Returns whether point is contained by this box
   */
  def contains(point: Point): Boolean = {
    x <= point.x && point.x <= x2 && y <= point.y && point.y <= y2
  }

  /**
   * Returns a new shrinked box from this box by the given epsilon (amount)
   */
  def shrink(amount: Double): Rectangle = {
    Rectangle(x + amount, y + amount, x2 - amount, y2 - amount)
  }

  /**
   * Returns whether the rectangle contains the point and if it is not in the rectangle's border
   */
  def almostContains(point: Point): Boolean = {
    x < point.x && point.x < x2 && y < point.y && point.y < y2
  }

}