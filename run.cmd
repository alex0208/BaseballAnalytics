call compile.cmd
call delete.cmd

call hadoop fs -mkdir input
call hadoop fs -put *.csv input
echo Finished adding files!
call hadoop jar BaseballAnalytics.jar BaseballAnalytics input output

call hadoop fs -cat output\part*