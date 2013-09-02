#!/bin/sh
exec scala "$0" "$@"

!#
import scala.collection.mutable.Map
import scala.util.Random
import scala.collection.mutable.ArrayBuffer
import java.io._

class IPGenerator(var sessionCount: Int, var sessionLength: Int) {
	var sessions = Map[String, Int]()

	def get_ip = {
		sessionGc
		sessionCreate
		val ip = sessions.keys.toSeq(Random.nextInt(sessions.size))
		sessions(ip) += 1
		ip
	}

	private def sessionCreate = {
		while(sessions.size < sessionCount) {
			sessions(randomIp) = 0
		}
	}

	private def sessionGc = {
		for((ip, count) <- sessions) {
			if (count >= sessionLength) sessions.remove(ip)
		}
	}

	private def randomIp: String = {
		val random = Random
		var octets = ArrayBuffer[Int]()
		octets += random.nextInt(223) + 1
		(1 to 3).foreach { _ => octets += random.nextInt(255) }
		octets.mkString(".")
	}
}

class LogGenerator(val ipGenObj: IPGenerator, var messagesCount: Int = 1) {
	val EXTENSIONS = 	Map(
			"html" -> 40,
			"php" -> 30,
			"png" -> 15,
			"gif" -> 10,
			"css" -> 5
		)

	val RESPONSE_CODES = Map(
			"200" -> 92,
			"404" -> 5,
			"503" -> 3
		)

	val USER_AGENTS = Map(
			"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1; SV1; .NET CLR 1.1.4322)" -> 30,
	    "Mozilla/5.0 (X11; Linux i686) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.50 Safari/534.24" -> 30,
  	  "Mozilla/5.0 (X11; Linux x86_64; rv:6.0a1) Gecko/20110421 Firefox/6.0a1" -> 40
		)

	val random = Random

	def writeMps(dest: FileWriter, mps: Int) = {
		val sleepTime = 1000 / mps
		while(true) {
			write(dest)
			Thread.sleep(sleepTime)
			messagesCount += 1
			print(s"\rMessages Count: ${messagesCount}")
		}
	}

	def write(dest: FileWriter) = {
		val ip = ipGenObj.get_ip
		val ext = pickWeightedKey(EXTENSIONS)
		val resp_code = pickWeightedKey(RESPONSE_CODES)
		val resp_size = random.nextInt(2 * 1024) + 192;
		val ua = pickWeightedKey(USER_AGENTS)
		val format = new java.text.SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss Z")
		val date = format.format(new java.util.Date())
		try { 
			val bw = new BufferedWriter(dest)
			bw.write(s"${ip} - - [${date}]" + " \"GET /test." + ext + " HTTP/1.1\" " +
				s"${resp_code} ${resp_size} " + "\"-\" \"" + ua + "\"\n")	
			bw.flush()	  
		} catch {
		  case e: Exception => e.printStackTrace
		}
	}

	private def pickWeightedKey(map: Map[String, Int]): String = {
		var total = 0
		map.values.foreach { weight => total += weight }
		val rand = Random.nextInt(total)
		var running = 0
		for((key, weight) <- map) {
			if(rand >= running && rand < (running + weight)) {
				return key
			}
			running += weight
		}
		map.keys.head
	}
}


val usage = """
Usage: random_gen file_path mps

	file_path - path of the file where to write the log events to
	mps 			- messages to generate per second

Random Http log events generator, simulates generating log events with random_ip(s)
"""

if(args.length != 2) {
	println(usage)
	System.exit(1)
}
val outputFile = new File(args(0))
val messagesPerSec = args(1).toInt
val outputFileWriter = new FileWriter(outputFile)

sys.addShutdownHook {
	println("")
	println("Caught ^C, Aborting ...")
	println("closing file")
	outputFileWriter.close()
}

try { 
	println(s"Generating random log events to ${outputFile} @ ${messagesPerSec}/second")	
	println("Press Ctrl-C to abort")
	new LogGenerator(new IPGenerator(100, 10)).writeMps(outputFileWriter, messagesPerSec)
} catch {
  case e: Exception => e.printStackTrace
} finally {
	println("closing file")
	outputFileWriter.close()
}