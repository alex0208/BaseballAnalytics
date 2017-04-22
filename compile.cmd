del BaseballAnalytics.jar

javac -classpath ../lib/hadoop-mapreduce-client-core-2.7.1.jar;../lib/hadoop-common-2.7.1.jar BaseballAnalytics.java

jar cf BaseballAnalytics.jar *.class

del *.class
