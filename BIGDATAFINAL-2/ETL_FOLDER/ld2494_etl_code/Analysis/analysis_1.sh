rm *.class
rm *.jar

hdfs dfs -rm -r -f hw

javac -classpath `yarn classpath` -d . A1Mapper.java
javac -classpath `yarn classpath` -d . A1Combiner.java
javac -classpath `yarn classpath` -d . A1Reducer.java
javac -classpath `yarn classpath`:. -d . A1.java

jar -cvf A1.jar *.class

hdfs dfs -mkdir hw
hdfs dfs -mkdir hw/clean_data
hdfs dfs -put clean_data.txt hw/clean_data
hdfs dfs -ls hw/clean_data

hadoop jar A1.jar A1 hw/clean_data/clean_data.txt ./hw/output

rm analysis.txt
hdfs dfs -get hw/output/part-r-00000 analysis.txt
cat analysis.txt