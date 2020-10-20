cd `pwd`
cd src/
hadoop com.sun.tools.javac.Main *.java
jar cf ../exercise_1.jar *.class
cd ..
hadoop jar exercise_1.jar ChiSquareCalculation /user/pknees/amazon-reviews/full/reviewscombined.json /user/e11946217/Exercise1/Results -top 200 -skip /user/e11946217/Exercise1/stopwords
hadoop fs -cat /user/e11946217/Exercise1/Results/part-r-00000 /user/e11946217/Exercise1/Results/all_terms/part-r-00000 | hadoop fs -put - /user/e11946217/Exercise1/Results
hadoop fs -getmerge /user/e11946217/Exercise1/Results/- ~/Exercise1/output.txt
