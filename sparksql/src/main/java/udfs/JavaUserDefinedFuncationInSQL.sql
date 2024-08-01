
-- 自定义函数在sql中的使用方式：



-- Compile and place UDAF MyAverage in a JAR file called `MyAverage.jar` in /tmp.
# CREATE FUNCTION myAverage AS 'MyAverage' USING JAR '/tmp/MyAverage.jar';
#
# SHOW USER FUNCTIONS;
# +------------------+
# |          function|
# +------------------+
# | default.myAverage|
# +------------------+
#
# CREATE TEMPORARY VIEW employees
# USING org.apache.spark.sql.json
# OPTIONS (
#     path "examples/src/main/resources/employees.json"
# );
#
# SELECT * FROM employees;
# +-------+------+
# |   name|salary|
# +-------+------+
# |Michael|  3000|
# |   Andy|  4500|
# | Justin|  3500|
# |  Berta|  4000|
# +-------+------+
#
# SELECT myAverage(salary) as average_salary FROM employees;
# +--------------+
# |average_salary|
# +--------------+
# |        3750.0|
# +--------------+


# 此外，spark还可以使用hive中的udf，udaf，udtf

-- Register `GenericUDTFExplode` and use it in Spark SQL
# CREATE TEMPORARY FUNCTION hiveUDTF
#     AS 'org.apache.hadoop.hive.ql.udf.generic.GenericUDTFExplode';
#
# SELECT * FROM t;
# +------+
# | value|
# +------+
# |[1, 2]|
# |[3, 4]|
# +------+
#
# SELECT hiveUDTF(value) FROM t;
# +---+
# |col|
# +---+
# |  1|
# |  2|
# |  3|
# |  4|
# +---+