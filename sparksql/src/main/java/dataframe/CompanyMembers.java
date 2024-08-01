package dataframe;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class CompanyMembers {

    private String name;
    private Long age;
    private String secretIdentity;
    private String[] powers;
}
