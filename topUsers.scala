val lines = sc.textFile("twitter.edges") 
val splitLines = lines.map(line => line.split(": "))
val pair = splitLines.flatMap( arr => {
	val value = arr(0)
	val keys = arr(1)
	val key = keys.split(",")
	key.map ( key => (key, 1))
	})
val followCounts = pair.reduceByKey((a,b) => a+b ).filter( _._2 > 1000).sortBy( _._2)
followCounts.saveAsTextFile("output")

System.exit(0)
