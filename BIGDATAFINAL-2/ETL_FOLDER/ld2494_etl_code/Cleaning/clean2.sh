rm *.class
rm *.jar

hdfs dfs -rm -r -f hw

javac -classpath `yarn classpath` -d . Clean2Mapper.java
javac -classpath `yarn classpath` -d . Clean2Reducer.java
javac -classpath `yarn classpath`:. -d . Clean2.java

jar -cvf Clean2.jar *.class

hdfs dfs -mkdir hw
hdfs dfs -mkdir hw/input
hdfs dfs -put new_data.txt hw/input
hdfs dfs -ls hw/input

hadoop jar Clean2.jar Clean2 hw/input/new_data.txt ./hw/output

rm clean_data.txt
hdfs dfs -get hw/output/part-r-00000 clean_data.txt
head -n 10 clean_data.txt