# auto increment field
This artical will talk the changes in eloq with mysql for auto increment field. In eloq, we did as our best to follow mysql, but eloq is a distributed database, so it must have some changes. Below are the changes:<br>
# 
1. In eloq, for auto increment field, all input value will be neglect and will get value from auto increment sequences to replace.
2. In eloq, the default value for auto increment offset is 1, auto increment increment is 1 and auto increment range is 256 (range is to apply how many ids from sequences and saved in local node cache one time).
3. In eloq, auto increment offset can not be changed when create table or use syntax in mysql to change the value of offset and increment.
4. Here added a new syntax to change the paramenters.<br>
    set global eloq_auto_increment="table_name=t1;offset=10000;increment=15;range=1024";
    table_name: The table name to change auto imcrement parameters. Now one talbe only has one auto increment field, so table name is enough. Here can be include database or not, for example: t1 or ./test/t1 are all right. If the table not in current database, db name is need.
    offset: The start id for this sequences.
    increment: The step between two neighboring records.
    range: To get how many ids one time from the sequences and save in local node memory cache to increment efficiency.
5. Now only support to change the auto increment parameters before to apply any ids from the sequences, or it will return error.
6. You can update one, two or all of three parameters one time.