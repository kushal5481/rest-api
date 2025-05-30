error id: 4F10CB8217C3F28BAEDD294F042CDC95
file:///J:/Projects/experian-prime-ingest/experian-prime-ingest-core/src/main/scala/com/experian/ind/nextgen/filevalidation/FileValidator.scala
### scala.reflect.internal.FatalError: ThisType(<none>) for sym which is not a class

occurred in the presentation compiler.

Failed to shorten type Iterable[_ <: databind.Module]

action parameters:
offset: 1021
uri: file:///J:/Projects/experian-prime-ingest/experian-prime-ingest-core/src/main/scala/com/experian/ind/nextgen/filevalidation/FileValidator.scala
text:
```scala
package com.experian.ind.nextgen.filevalidation

import com.typesafe.config.ConfigFactory
import java.io.InputStream
import com.fasterxml.jackson.databind.ObjectMapper

object FileValidator{
 def main(args: Array[String]): Unit = {
        val config = ConfigFactory.load()
        val sparkConfig = config.getConfig("spark")
        val appName = sparkConfig.getString("appName")
        val master = sparkConfig.getString("master")
        val tempDir = sparkConfig.getString("tempDir")

        println("\n Welcome to FileValidator")
        val spark = SparkSession.builder().appName(appName).master(master).config("spark.local.dir",tempDir).getOrCreate()

        val resourceStream: InputStream = getClass().getClassLoader().getResourceAsStream("file-config.json")
        if (resourceStream == null){
            println("file-config.json not found in resources folder.")
            return
        }

        //creating jackson mapper
        val mapper = new ObjectMapper with ScalaObject@@

 }
}
```


presentation compiler configuration:
Scala version: 2.12.18
Classpath:
<WORKSPACE>\experian-prime-ingest-core\target\bloop-bsp-clients-classes\classes-Metals-b_Jhcv1vRNGR0rNWd8rqXg== [missing ], <HOME>\AppData\Local\Coursier\cache\v1\https\repo1.maven.org\maven2\com\sourcegraph\semanticdb-javac\0.10.4\semanticdb-javac-0.10.4.jar [exists ], <WORKSPACE>\target\bloop-bsp-clients-classes\classes-Metals-b_Jhcv1vRNGR0rNWd8rqXg== [missing ], <WORKSPACE>\experian-prime-ingest-core\target\classes [exists ], <HOME>\.m2\repository\org\apache\spark\spark-core_2.12\3.3.1\spark-core_2.12-3.3.1.jar [exists ], <HOME>\.m2\repository\org\apache\avro\avro\1.11.0\avro-1.11.0.jar [exists ], <HOME>\.m2\repository\com\fasterxml\jackson\core\jackson-core\2.12.5\jackson-core-2.12.5.jar [exists ], <HOME>\.m2\repository\org\apache\commons\commons-compress\1.21\commons-compress-1.21.jar [exists ], <HOME>\.m2\repository\org\apache\avro\avro-mapred\1.11.0\avro-mapred-1.11.0.jar [exists ], <HOME>\.m2\repository\org\apache\avro\avro-ipc\1.11.0\avro-ipc-1.11.0.jar [exists ], <HOME>\.m2\repository\org\tukaani\xz\1.9\xz-1.9.jar [exists ], <HOME>\.m2\repository\com\twitter\chill_2.12\0.10.0\chill_2.12-0.10.0.jar [exists ], <HOME>\.m2\repository\com\esotericsoftware\kryo-shaded\4.0.2\kryo-shaded-4.0.2.jar [exists ], <HOME>\.m2\repository\com\esotericsoftware\minlog\1.3.0\minlog-1.3.0.jar [exists ], <HOME>\.m2\repository\org\objenesis\objenesis\2.5.1\objenesis-2.5.1.jar [exists ], <HOME>\.m2\repository\com\twitter\chill-java\0.10.0\chill-java-0.10.0.jar [exists ], <HOME>\.m2\repository\org\apache\xbean\xbean-asm9-shaded\4.20\xbean-asm9-shaded-4.20.jar [exists ], <HOME>\.m2\repository\org\apache\hadoop\hadoop-client-api\3.3.2\hadoop-client-api-3.3.2.jar [exists ], <HOME>\.m2\repository\org\apache\hadoop\hadoop-client-runtime\3.3.2\hadoop-client-runtime-3.3.2.jar [exists ], <HOME>\.m2\repository\org\apache\spark\spark-launcher_2.12\3.3.1\spark-launcher_2.12-3.3.1.jar [exists ], <HOME>\.m2\repository\org\apache\spark\spark-kvstore_2.12\3.3.1\spark-kvstore_2.12-3.3.1.jar [exists ], <HOME>\.m2\repository\org\fusesource\leveldbjni\leveldbjni-all\1.8\leveldbjni-all-1.8.jar [exists ], <HOME>\.m2\repository\com\fasterxml\jackson\core\jackson-annotations\2.13.4\jackson-annotations-2.13.4.jar [exists ], <HOME>\.m2\repository\org\rocksdb\rocksdbjni\6.20.3\rocksdbjni-6.20.3.jar [exists ], <HOME>\.m2\repository\org\apache\spark\spark-network-common_2.12\3.3.1\spark-network-common_2.12-3.3.1.jar [exists ], <HOME>\.m2\repository\com\google\crypto\tink\tink\1.6.1\tink-1.6.1.jar [exists ], <HOME>\.m2\repository\com\google\protobuf\protobuf-java\3.14.0\protobuf-java-3.14.0.jar [exists ], <HOME>\.m2\repository\com\google\code\gson\gson\2.8.6\gson-2.8.6.jar [exists ], <HOME>\.m2\repository\org\apache\spark\spark-network-shuffle_2.12\3.3.1\spark-network-shuffle_2.12-3.3.1.jar [exists ], <HOME>\.m2\repository\org\apache\spark\spark-unsafe_2.12\3.3.1\spark-unsafe_2.12-3.3.1.jar [exists ], <HOME>\.m2\repository\javax\activation\activation\1.1.1\activation-1.1.1.jar [exists ], <HOME>\.m2\repository\org\apache\curator\curator-recipes\2.13.0\curator-recipes-2.13.0.jar [exists ], <HOME>\.m2\repository\org\apache\curator\curator-framework\2.13.0\curator-framework-2.13.0.jar [exists ], <HOME>\.m2\repository\org\apache\curator\curator-client\2.13.0\curator-client-2.13.0.jar [exists ], <HOME>\.m2\repository\com\google\guava\guava\16.0.1\guava-16.0.1.jar [exists ], <HOME>\.m2\repository\org\apache\zookeeper\zookeeper\3.6.2\zookeeper-3.6.2.jar [exists ], <HOME>\.m2\repository\commons-lang\commons-lang\2.6\commons-lang-2.6.jar [exists ], <HOME>\.m2\repository\org\apache\zookeeper\zookeeper-jute\3.6.2\zookeeper-jute-3.6.2.jar [exists ], <HOME>\.m2\repository\org\apache\yetus\audience-annotations\0.5.0\audience-annotations-0.5.0.jar [exists ], <HOME>\.m2\repository\jakarta\servlet\jakarta.servlet-api\4.0.3\jakarta.servlet-api-4.0.3.jar [exists ], <HOME>\.m2\repository\commons-codec\commons-codec\1.15\commons-codec-1.15.jar [exists ], <HOME>\.m2\repository\org\apache\commons\commons-lang3\3.12.0\commons-lang3-3.12.0.jar [exists ], <HOME>\.m2\repository\org\apache\commons\commons-math3\3.6.1\commons-math3-3.6.1.jar [exists ], <HOME>\.m2\repository\org\apache\commons\commons-text\1.9\commons-text-1.9.jar [exists ], <HOME>\.m2\repository\commons-io\commons-io\2.11.0\commons-io-2.11.0.jar [exists ], <HOME>\.m2\repository\commons-collections\commons-collections\3.2.2\commons-collections-3.2.2.jar [exists ], <HOME>\.m2\repository\org\apache\commons\commons-collections4\4.4\commons-collections4-4.4.jar [exists ], <HOME>\.m2\repository\com\google\code\findbugs\jsr305\3.0.0\jsr305-3.0.0.jar [exists ], <HOME>\.m2\repository\org\slf4j\slf4j-api\1.7.32\slf4j-api-1.7.32.jar [exists ], <HOME>\.m2\repository\org\slf4j\jul-to-slf4j\1.7.32\jul-to-slf4j-1.7.32.jar [exists ], <HOME>\.m2\repository\org\slf4j\jcl-over-slf4j\1.7.32\jcl-over-slf4j-1.7.32.jar [exists ], <HOME>\.m2\repository\org\apache\logging\log4j\log4j-slf4j-impl\2.17.2\log4j-slf4j-impl-2.17.2.jar [exists ], <HOME>\.m2\repository\org\apache\logging\log4j\log4j-api\2.17.2\log4j-api-2.17.2.jar [exists ], <HOME>\.m2\repository\org\apache\logging\log4j\log4j-core\2.17.2\log4j-core-2.17.2.jar [exists ], <HOME>\.m2\repository\org\apache\logging\log4j\log4j-1.2-api\2.17.2\log4j-1.2-api-2.17.2.jar [exists ], <HOME>\.m2\repository\com\ning\compress-lzf\1.1\compress-lzf-1.1.jar [exists ], <HOME>\.m2\repository\org\xerial\snappy\snappy-java\1.1.8.4\snappy-java-1.1.8.4.jar [exists ], <HOME>\.m2\repository\org\lz4\lz4-java\1.8.0\lz4-java-1.8.0.jar [exists ], <HOME>\.m2\repository\com\github\luben\zstd-jni\1.5.2-1\zstd-jni-1.5.2-1.jar [exists ], <HOME>\.m2\repository\org\roaringbitmap\RoaringBitmap\0.9.25\RoaringBitmap-0.9.25.jar [exists ], <HOME>\.m2\repository\org\scala-lang\modules\scala-xml_2.12\1.2.0\scala-xml_2.12-1.2.0.jar [exists ], <HOME>\.m2\repository\org\scala-lang\scala-reflect\2.12.15\scala-reflect-2.12.15.jar [exists ], <HOME>\.m2\repository\org\json4s\json4s-jackson_2.12\3.7.0-M11\json4s-jackson_2.12-3.7.0-M11.jar [exists ], <HOME>\.m2\repository\org\json4s\json4s-core_2.12\3.7.0-M11\json4s-core_2.12-3.7.0-M11.jar [exists ], <HOME>\.m2\repository\org\json4s\json4s-ast_2.12\3.7.0-M11\json4s-ast_2.12-3.7.0-M11.jar [exists ], <HOME>\.m2\repository\org\json4s\json4s-scalap_2.12\3.7.0-M11\json4s-scalap_2.12-3.7.0-M11.jar [exists ], <HOME>\.m2\repository\org\glassfish\jersey\core\jersey-client\2.36\jersey-client-2.36.jar [exists ], <HOME>\.m2\repository\jakarta\ws\rs\jakarta.ws.rs-api\2.1.6\jakarta.ws.rs-api-2.1.6.jar [exists ], <HOME>\.m2\repository\org\glassfish\hk2\external\jakarta.inject\2.6.1\jakarta.inject-2.6.1.jar [exists ], <HOME>\.m2\repository\org\glassfish\jersey\core\jersey-common\2.36\jersey-common-2.36.jar [exists ], <HOME>\.m2\repository\jakarta\annotation\jakarta.annotation-api\1.3.5\jakarta.annotation-api-1.3.5.jar [exists ], <HOME>\.m2\repository\org\glassfish\hk2\osgi-resource-locator\1.0.3\osgi-resource-locator-1.0.3.jar [exists ], <HOME>\.m2\repository\org\glassfish\jersey\core\jersey-server\2.36\jersey-server-2.36.jar [exists ], <HOME>\.m2\repository\jakarta\validation\jakarta.validation-api\2.0.2\jakarta.validation-api-2.0.2.jar [exists ], <HOME>\.m2\repository\org\glassfish\jersey\containers\jersey-container-servlet\2.36\jersey-container-servlet-2.36.jar [exists ], <HOME>\.m2\repository\org\glassfish\jersey\containers\jersey-container-servlet-core\2.36\jersey-container-servlet-core-2.36.jar [exists ], <HOME>\.m2\repository\org\glassfish\jersey\inject\jersey-hk2\2.36\jersey-hk2-2.36.jar [exists ], <HOME>\.m2\repository\org\glassfish\hk2\hk2-locator\2.6.1\hk2-locator-2.6.1.jar [exists ], <HOME>\.m2\repository\org\glassfish\hk2\external\aopalliance-repackaged\2.6.1\aopalliance-repackaged-2.6.1.jar [exists ], <HOME>\.m2\repository\org\glassfish\hk2\hk2-api\2.6.1\hk2-api-2.6.1.jar [exists ], <HOME>\.m2\repository\org\glassfish\hk2\hk2-utils\2.6.1\hk2-utils-2.6.1.jar [exists ], <HOME>\.m2\repository\org\javassist\javassist\3.25.0-GA\javassist-3.25.0-GA.jar [exists ], <HOME>\.m2\repository\io\netty\netty-all\4.1.74.Final\netty-all-4.1.74.Final.jar [exists ], <HOME>\.m2\repository\io\netty\netty-buffer\4.1.74.Final\netty-buffer-4.1.74.Final.jar [exists ], <HOME>\.m2\repository\io\netty\netty-codec\4.1.74.Final\netty-codec-4.1.74.Final.jar [exists ], <HOME>\.m2\repository\io\netty\netty-common\4.1.74.Final\netty-common-4.1.74.Final.jar [exists ], <HOME>\.m2\repository\io\netty\netty-handler\4.1.74.Final\netty-handler-4.1.74.Final.jar [exists ], <HOME>\.m2\repository\io\netty\netty-tcnative-classes\2.0.48.Final\netty-tcnative-classes-2.0.48.Final.jar [exists ], <HOME>\.m2\repository\io\netty\netty-resolver\4.1.74.Final\netty-resolver-4.1.74.Final.jar [exists ], <HOME>\.m2\repository\io\netty\netty-transport\4.1.74.Final\netty-transport-4.1.74.Final.jar [exists ], <HOME>\.m2\repository\io\netty\netty-transport-classes-epoll\4.1.74.Final\netty-transport-classes-epoll-4.1.74.Final.jar [exists ], <HOME>\.m2\repository\io\netty\netty-transport-native-unix-common\4.1.74.Final\netty-transport-native-unix-common-4.1.74.Final.jar [exists ], <HOME>\.m2\repository\io\netty\netty-transport-classes-kqueue\4.1.74.Final\netty-transport-classes-kqueue-4.1.74.Final.jar [exists ], <HOME>\.m2\repository\com\clearspring\analytics\stream\2.9.6\stream-2.9.6.jar [exists ], <HOME>\.m2\repository\io\dropwizard\metrics\metrics-core\4.2.7\metrics-core-4.2.7.jar [exists ], <HOME>\.m2\repository\io\dropwizard\metrics\metrics-jvm\4.2.7\metrics-jvm-4.2.7.jar [exists ], <HOME>\.m2\repository\io\dropwizard\metrics\metrics-json\4.2.7\metrics-json-4.2.7.jar [exists ], <HOME>\.m2\repository\io\dropwizard\metrics\metrics-graphite\4.2.7\metrics-graphite-4.2.7.jar [exists ], <HOME>\.m2\repository\io\dropwizard\metrics\metrics-jmx\4.2.7\metrics-jmx-4.2.7.jar [exists ], <HOME>\.m2\repository\com\fasterxml\jackson\core\jackson-databind\2.13.4.1\jackson-databind-2.13.4.1.jar [exists ], <HOME>\.m2\repository\com\fasterxml\jackson\module\jackson-module-scala_2.12\2.13.4\jackson-module-scala_2.12-2.13.4.jar [exists ], <HOME>\.m2\repository\com\thoughtworks\paranamer\paranamer\2.8\paranamer-2.8.jar [exists ], <HOME>\.m2\repository\org\apache\ivy\ivy\2.5.0\ivy-2.5.0.jar [exists ], <HOME>\.m2\repository\oro\oro\2.0.8\oro-2.0.8.jar [exists ], <HOME>\.m2\repository\net\razorvine\pickle\1.2\pickle-1.2.jar [exists ], <HOME>\.m2\repository\net\sf\py4j\py4j\0.10.9.5\py4j-0.10.9.5.jar [exists ], <HOME>\.m2\repository\org\apache\spark\spark-tags_2.12\3.3.1\spark-tags_2.12-3.3.1.jar [exists ], <HOME>\.m2\repository\org\apache\commons\commons-crypto\1.1.0\commons-crypto-1.1.0.jar [exists ], <HOME>\.m2\repository\org\spark-project\spark\unused\1.0.0\unused-1.0.0.jar [exists ], <HOME>\.m2\repository\org\scala-lang\scala-library\2.12.18\scala-library-2.12.18.jar [exists ], <HOME>\.m2\repository\org\scala-lang\scala-library\2.12.18\scala-library-2.12.18.jar [exists ]
Options:
-Yrangepos -Xplugin-require:semanticdb




#### Error stacktrace:

```
scala.reflect.internal.Reporting.abort(Reporting.scala:69)
	scala.reflect.internal.Reporting.abort$(Reporting.scala:65)
	scala.reflect.internal.SymbolTable.abort(SymbolTable.scala:28)
	scala.reflect.internal.Types$ThisType.<init>(Types.scala:1193)
	scala.reflect.internal.Types$UniqueThisType.<init>(Types.scala:1213)
	scala.reflect.internal.Types$ThisType$.apply(Types.scala:1217)
	scala.meta.internal.pc.MetalsGlobal.loop$1(MetalsGlobal.scala:401)
	scala.meta.internal.pc.MetalsGlobal.$anonfun$shortType$9(MetalsGlobal.scala:417)
	scala.collection.immutable.List.map(List.scala:293)
	scala.meta.internal.pc.MetalsGlobal.loop$1(MetalsGlobal.scala:417)
	scala.meta.internal.pc.MetalsGlobal.loop$1(MetalsGlobal.scala:514)
	scala.meta.internal.pc.MetalsGlobal.shortType(MetalsGlobal.scala:545)
	scala.meta.internal.pc.Signatures$SignaturePrinter.paramLabel(Signatures.scala:419)
	scala.meta.internal.pc.Signatures$SignaturePrinter.$anonfun$defaultMethodSignature$2(Signatures.scala:366)
	scala.collection.immutable.List.flatMap(List.scala:366)
	scala.meta.internal.pc.Signatures$SignaturePrinter.$anonfun$defaultMethodSignature$1(Signatures.scala:362)
	scala.collection.Iterator$$anon$11.nextCur(Iterator.scala:486)
	scala.collection.Iterator$$anon$11.hasNext(Iterator.scala:492)
	scala.collection.Iterator$$anon$18.hasNext(Iterator.scala:850)
	scala.collection.Iterator$$anon$10.hasNext(Iterator.scala:460)
	scala.collection.Iterator.foreach(Iterator.scala:943)
	scala.collection.Iterator.foreach$(Iterator.scala:943)
	scala.collection.AbstractIterator.foreach(Iterator.scala:1431)
	scala.collection.TraversableOnce.addString(TraversableOnce.scala:424)
	scala.collection.TraversableOnce.addString$(TraversableOnce.scala:407)
	scala.collection.AbstractIterator.addString(Iterator.scala:1431)
	scala.collection.TraversableOnce.mkString(TraversableOnce.scala:377)
	scala.collection.TraversableOnce.mkString$(TraversableOnce.scala:376)
	scala.collection.AbstractIterator.mkString(Iterator.scala:1431)
	scala.meta.internal.pc.Signatures$SignaturePrinter.methodSignature(Signatures.scala:397)
	scala.meta.internal.pc.Signatures$SignaturePrinter.defaultMethodSignature(Signatures.scala:374)
	scala.meta.internal.pc.completions.OverrideCompletions$OverrideCandidate$1.signature(OverrideCompletions.scala:299)
	scala.meta.internal.pc.completions.OverrideCompletions$OverrideCandidate$1.label(OverrideCompletions.scala:297)
	scala.meta.internal.pc.completions.OverrideCompletions$OverrideCandidate$1.toMember(OverrideCompletions.scala:283)
	scala.meta.internal.pc.completions.OverrideCompletions.$anonfun$getMembers$5(OverrideCompletions.scala:314)
	scala.collection.immutable.List.map(List.scala:297)
	scala.meta.internal.pc.completions.OverrideCompletions.scala$meta$internal$pc$completions$OverrideCompletions$$getMembers(OverrideCompletions.scala:314)
	scala.meta.internal.pc.completions.OverrideCompletions$OverrideCompletion.contribute(OverrideCompletions.scala:73)
	scala.meta.internal.pc.CompletionProvider.filterInteresting(CompletionProvider.scala:416)
	scala.meta.internal.pc.CompletionProvider.safeCompletionsAt(CompletionProvider.scala:583)
	scala.meta.internal.pc.CompletionProvider.completions(CompletionProvider.scala:61)
	scala.meta.internal.pc.ScalaPresentationCompiler.$anonfun$complete$1(ScalaPresentationCompiler.scala:225)
```
#### Short summary: 

scala.reflect.internal.FatalError: ThisType(<none>) for sym which is not a class