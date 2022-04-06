package com.nttdata.bulk;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.ToString;
import lombok.experimental.FieldDefaults;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@FieldDefaults(level = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
@Getter
public class User {

    /*
    {
      "department": "C.S/NO.RLO",
      "uid": "EUAR1913",
      "manager": "uid=01045892,OU=Dipendenti,OU=Telecomitalia,O=Telecom Italia Group",
      "cn": "EUAR1913",
      "mail": null,
      "displayName": null
    }
    */
    String department;
    String uid;
    String manager;
    String cn;
    String mail;
    String displayName;
}
