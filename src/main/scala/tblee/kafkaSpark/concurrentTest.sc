import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global

def quickSort[T](xxs: List[T])(implicit ord: Ordering[T]): List[T] = {
  xxs match {
    case Nil => Nil
    case x::xs =>
      val smaller = quickSort(xs.filter(z => ord.lteq(z, x)))
      val larger = quickSort(xs.filter(z => ord.gt(z, x)))
      smaller ::: List(x) ::: larger
  }
}

quickSort(List(9, 8, 7, 6, 5, 4, 3, 2, 1))
quickSort(List("dfji", "zfd", "d", "pofa"))
