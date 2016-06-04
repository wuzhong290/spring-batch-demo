package com.spring.batch;


import org.springframework.jdbc.core.RowMapper;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Created by wuzhong on 2016/5/30.
 */
public class UserIdRowMapper  implements RowMapper<String> {
    @Override
    public String mapRow(ResultSet rs, int rowNum) throws SQLException {
        return rs.getString("user_id");
    }
}
