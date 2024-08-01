package dataframe;


import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Company {

    private String squadName;
    private String homeTown;
    private Long formed;
    private String secretBase;
    private boolean active;
    private CompanyMembers[] members;
}
