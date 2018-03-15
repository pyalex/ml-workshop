name := "gmail-test"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "com.google.api-client" % "google-api-client" % "1.22.0" exclude("com.google.guava", "guava-jdk5")
libraryDependencies += "com.google.apis" % "google-api-services-gmail" % "v1-rev82-1.22.0"
libraryDependencies += "javax.mail" % "mail" % "1.4.7"
libraryDependencies += "org.jsoup" % "jsoup" % "1.11.2"
libraryDependencies += "org.slf4j" % "slf4j-simple" % "1.7.21"
libraryDependencies += "de.javakaffee" % "kryo-serializers" % "0.42"

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % "0.4.7",
  "com.spotify" %% "scio-tensorflow" % "0.4.7",
  "com.spotify" %% "scio-test" % "0.4.7" % "test",
  "org.apache.beam" % "beam-sdks-java-core" % "2.3.0",
  "org.apache.beam" % "beam-runners-direct-java" % "2.3.0",
  "org.apache.beam" % "beam-runners-google-cloud-dataflow-java" % "2.3.0"
)

val tensorFlowVersion = "1.6.0"
val shapelessDatatypeVersion = "0.1.8"

libraryDependencies ++= Seq(
  "org.tensorflow" % "tensorflow" % tensorFlowVersion,
  "org.tensorflow" % "proto" % tensorFlowVersion,
  "me.lyh" %% "shapeless-datatype-tensorflow" % shapelessDatatypeVersion
//  "com.spotify" %% "featran-core" % featranVersion,
//  "com.spotify" %% "featran-scio" % featranVersion,
//  "com.spotify" %% "featran-tensorflow" % featranVersion
)

dependencyOverrides += "com.google.api-client" % "google-api-client" % "1.22.0"
dependencyOverrides += "com.google.guava" % "guava" % "20.0"


addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.0" cross CrossVersion.full)
