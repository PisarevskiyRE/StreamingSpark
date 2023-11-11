package chapter3

import java.io.PrintStream
import java.net.ServerSocket

object DataSender extends App {

  val socket = new ServerSocket(12345).accept()
  val printStream = new PrintStream(socket.getOutputStream)

  Thread.sleep(5000)
  printStream.println("d1,15.9")
  Thread.sleep(7000)
  printStream.println("d1,5.1")
  Thread.sleep(4000)
  printStream.println("d1,9.0")
  printStream.println("d2,21.9")
  Thread.sleep(1000)
  printStream.println("d2,8.1")
  Thread.sleep(2000)
  printStream.println("d2,2.4")
  printStream.println("d3,12.2")
  
}
