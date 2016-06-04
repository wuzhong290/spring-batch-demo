package com.spring.batch;

import org.apache.log4j.Logger;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.support.SqlServerPagingQueryProvider;
import org.springframework.batch.repeat.RepeatCallback;
import org.springframework.batch.repeat.RepeatContext;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.batch.repeat.support.TaskExecutorRepeatTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by wuzhong on 2016/6/4.
 */
public class Main {

    private static Logger logger = Logger.getLogger(Main.class);

    private static JdbcTemplate jdbcDriver; //自己实例化一下

    public static void main(String[] args) {
        SqlServerPagingQueryProvider provider = new SqlServerPagingQueryProvider();
        provider.setFromClause("(select distinct user_id from tb_order_info where cancel_reason_1 = '未取消' and firm_time >'2015-01-01' and firm_time <'2015-02-01') AS id1");
        provider.setSelectClause("user_id");
        Map<String, Order> sortKeys = new HashMap<String, Order>();
        sortKeys.put("user_id", Order.ASCENDING);
        provider.setSortKeys(sortKeys);

        final JdbcPagingItemReader<String> reader = new JdbcPagingItemReader<String>();
        reader.setJdbcTemplate(jdbcDriver);
        reader.setQueryProvider(provider);
        reader.setPageSize(10000);
        reader.setRowMapper(new UserIdRowMapper());

        try {
            reader.afterPropertiesSet();
        } catch (Exception e) {
            e.printStackTrace();
        }


        TaskExecutorRepeatTemplate repeatTemplate = new TaskExecutorRepeatTemplate();
        repeatTemplate.setTaskExecutor(new SimpleAsyncTaskExecutor("user-order"));
        repeatTemplate.setThrottleLimit(3);

        reader.open(new ExecutionContext());

        repeatTemplate.iterate(new RepeatCallback() {

            @Override
            public RepeatStatus doInIteration(RepeatContext context) throws Exception {
                boolean flag = true;
                RepeatStatus status = RepeatStatus.CONTINUABLE;
                List<Object[]> idItems = new ArrayList<Object[]>();
                List<Integer> ids = new ArrayList<Integer>();
                int num = 0;
                while (flag) {
                    String id = null;
                    try {
                        id = reader.read();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    if (null == id) {
                        logger.info("数据已经没有了" + Thread.currentThread().getName()+":"+num);
                        flag = false;
                        if (ids.size() > 0) {
                            //持久化
                            ids.clear();
                            idItems.clear();
                        }
                        status = RepeatStatus.FINISHED;
                    } else {
                        ++num;
                        ids.add(Integer.parseInt(id));
                        if (ids.size() == 2000) {
                            //持久化
                            ids.clear();
                            idItems.clear();
                        }
                    }
                }
                return status;
            }
        });

        reader.close();
    }
}
