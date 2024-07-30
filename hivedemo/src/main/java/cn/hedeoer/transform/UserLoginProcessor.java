package cn.hedeoer.transform;
import java.io.*;
import java.util.*;
import com.google.gson.*;

// 定义类 UserLoginProcessor 以处理 Hive 输入的数据
public class UserLoginProcessor {

    // 内部类 ProvinceData，用于存储每个省份的用户集合
    static class ProvinceData {
        String province;  // 省份ID
        Set<String> user_list;  // 用户ID集合

        ProvinceData(String province) {
            this.province = province;
            this.user_list = new HashSet<>();
        }
    }

    // 内部类 DayData，用于存储每一天的省份数据
    static class DayData {
        String dt;  // 日期
        List<ProvinceData> provinces;  // 省份数据列表

        DayData(String dt) {
            this.dt = dt;
            this.provinces = new ArrayList<>();
        }
    }

    // 主方法，从标准输入读取数据，处理后输出JSON格式
    public static void main(String[] args) throws IOException {
//        String input = "user_id\tuser_type\tuser_name\tprovince_id\tcity_id\tisp_id\tlogin_time\tlogout_time\tdt\tlogin_date\n" +
//                "1\t1\t张三\t1\t1\t1\t1588963200\t1588963200\t20200501\t2020-05-01\n" +
//                "1\t1\t张三\t1\t1\t1\t1588963200\t1588963200\t20200501\t2020-05-01\n" +
//                "2\t1\t李四\t2\t2\t2\t1588963200\t1588963200\t20200501\t2020-05-01\n" +
//                "3\t1\t王五\t1\t1\t1\t1588963200\t1588963200\t20200501\t2020-05-01\n";
        BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
//        BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(input.getBytes("UTF-8"))));
        Map<String, DayData> resultMap = new HashMap<>();  // 存储结果的Map
        String line;

        // 逐行读取输入
        while ((line = reader.readLine()) != null) {
            String[] fields = line.split("\t");
            String user_id = fields[0];  // 用户ID
            String date_id = fields[1];  // 日期ID
            String province_id = fields[3];  // 省份ID
            String dt = fields[9];  // 登录日期

            // 获取或创建DayData对象
            DayData dayData = resultMap.get(dt);
            if (dayData == null) {
                dayData = new DayData(dt);
                resultMap.put(dt, dayData);
            }

            // 获取或创建ProvinceData对象
            ProvinceData provinceData = null;
            for (ProvinceData pd : dayData.provinces) {
                if (pd.province.equals(province_id)) {
                    provinceData = pd;
                    break;
                }
            }
            if (provinceData == null) {
                provinceData = new ProvinceData(province_id);
                dayData.provinces.add(provinceData);
            }

            // 添加用户ID到省份数据中
            provinceData.user_list.add(user_id);
        }

        Gson gson = new Gson();  // 使用Gson库将结果转换为JSON格式

        // 输出结果
        for (DayData dayData : resultMap.values()) {
            System.out.println(gson.toJson(dayData));
        }
    }
}
