package exceptions

case class InfiniteValidateException(val content:String,val cause: Throwable = None.orNull) extends RuntimeException(content,cause)
