package dataframe;

import org.apache.spark.sql.*;

import java.util.Collections;

public class ExplodeExample {
    /*
     * 读取sparksql/data/company.json，将属性members中的数组展开，实现类似sql中的explode的效果
     * */

    public static void main(String[] args) throws AnalysisException {

        SparkSession session = SparkSession.builder()
                .appName("explode")
                .master("local[*]")
                .config("spark.sql.debug.maxToStringFields", 1000)
                .getOrCreate();

        Dataset<Row> source = session.read().json("sparksql/data/company.json");

        Encoder<Company> companyEncoder = Encoders.bean(Company.class);

        source.createTempView("company");

        session.sql("SELECT \n" +
                "  squadName, \n" +
                "  homeTown, \n" +
                "  member.name, \n" +
                "  member.age, \n" +
                "  member.secretIdentity, \n" +
                "  member.powers,\n" +
                "  power\n" +
                "FROM \n" +
                "  company \n" +
                "LATERAL VIEW explode(members) AS member  \n" +
                "LATERAL VIEW explode(member.powers) AS power;\n").show(false);
    }


/*
* +----------------+----------+---------------+-------+--------------+-----------------------------------------------------------------------------+-----------------------+
|squadName       |homeTown  |name           |age    |secretIdentity|powers                                                                       |power                  |
+----------------+----------+---------------+-------+--------------+-----------------------------------------------------------------------------+-----------------------+
|Super hero squad|Metro City|Molecule Man   |29     |Dan Jukes     |[Radiation resistance, Turning tiny, Radiation blast]                        |Radiation resistance   |
|Super hero squad|Metro City|Molecule Man   |29     |Dan Jukes     |[Radiation resistance, Turning tiny, Radiation blast]                        |Turning tiny           |
|Super hero squad|Metro City|Molecule Man   |29     |Dan Jukes     |[Radiation resistance, Turning tiny, Radiation blast]                        |Radiation blast        |
|Super hero squad|Metro City|Madame Uppercut|39     |Jane Wilson   |[Million tonne punch, Damage resistance, Superhuman reflexes]                |Million tonne punch    |
|Super hero squad|Metro City|Madame Uppercut|39     |Jane Wilson   |[Million tonne punch, Damage resistance, Superhuman reflexes]                |Damage resistance      |
|Super hero squad|Metro City|Madame Uppercut|39     |Jane Wilson   |[Million tonne punch, Damage resistance, Superhuman reflexes]                |Superhuman reflexes    |
|Super hero squad|Metro City|Eternal Flame  |1000000|Unknown       |[Immortality, Heat Immunity, Inferno, Teleportation, Interdimensional travel]|Immortality            |
|Super hero squad|Metro City|Eternal Flame  |1000000|Unknown       |[Immortality, Heat Immunity, Inferno, Teleportation, Interdimensional travel]|Heat Immunity          |
|Super hero squad|Metro City|Eternal Flame  |1000000|Unknown       |[Immortality, Heat Immunity, Inferno, Teleportation, Interdimensional travel]|Inferno                |
|Super hero squad|Metro City|Eternal Flame  |1000000|Unknown       |[Immortality, Heat Immunity, Inferno, Teleportation, Interdimensional travel]|Teleportation          |
|Super hero squad|Metro City|Eternal Flame  |1000000|Unknown       |[Immortality, Heat Immunity, Inferno, Teleportation, Interdimensional travel]|Interdimensional travel|
+----------------+----------+---------------+-------+--------------+-----------------------------------------------------------------------------+-----------------------+
*
* */
}
