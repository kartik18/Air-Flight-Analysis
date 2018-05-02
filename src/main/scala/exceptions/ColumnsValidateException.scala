package exceptions

case class ColumnsValidateException(val content:String,val cause: Throwable = None.orNull) extends RuntimeException(content,cause)

