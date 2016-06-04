package com.spring.batch;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.database.AbstractPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.util.Assert;
import org.springframework.util.ClassUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;


public class JdbcPagingItemReader<T> extends AbstractPagingItemReader<T> implements InitializingBean {
    private static final String START_AFTER_VALUE = "start.after";

    public static final int VALUE_NOT_SET = -1;

    private JdbcTemplate jdbcTemplate;

    private PagingQueryProvider queryProvider;

    private Map<String, Object> parameterValues;

    private RowMapper<T> rowMapper;

    private String firstPageSql;

    private String remainingPagesSql;

    private Map<String, Object> startAfterValues;

    private Map<String, Object> previousStartAfterValues;

    private int fetchSize = VALUE_NOT_SET;

    private ThreadLocal<List<T>> resultInfo = new ThreadLocal<>();

    private ThreadLocal<Integer> currentInfo = new ThreadLocal<>();

    private Object lock = new Object();

    private volatile int page = 0;

    public JdbcPagingItemReader() {
        setName(ClassUtils.getShortName(JdbcPagingItemReader.class));
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * Gives the JDBC driver a hint as to the number of rows that should be
     * fetched from the database when more rows are needed for this
     * <code>ResultSet</code> object. If the fetch size specified is zero, the
     * JDBC driver ignores the value.
     *
     * @param fetchSize the number of rows to fetch
     * @see ResultSet#setFetchSize(int)
     */
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    /**
     * A {@link PagingQueryProvider}. Supplies all the platform dependent query
     * generation capabilities needed by the reader.
     *
     * @param queryProvider the {@link PagingQueryProvider} to use
     */
    public void setQueryProvider(PagingQueryProvider queryProvider) {
        this.queryProvider = queryProvider;
    }

    /**
     * The row mapper implementation to be used by this reader. The row mapper
     * is used to convert result set rows into objects, which are then returned
     * by the reader.
     *
     * @param rowMapper a
     * {@link org.springframework.jdbc.core.simple.ParameterizedRowMapper}
     * implementation
     */
    public void setRowMapper(RowMapper<T> rowMapper) {
        this.rowMapper = rowMapper;
    }

    /**
     * The parameter values to be used for the query execution. If you use named
     * parameters then the key should be the name used in the query clause. If
     * you use "?" placeholders then the key should be the relative index that
     * the parameter appears in the query string built using the select, from
     * and where clauses specified.
     *
     * @param parameterValues the values keyed by the parameter named/index used
     * in the query string.
     */
    public void setParameterValues(Map<String, Object> parameterValues) {
        this.parameterValues = parameterValues;
    }

    /**
     * Check mandatory properties.
     * @see InitializingBean#afterPropertiesSet()
     */
    @Override
    public void afterPropertiesSet() throws Exception {
        super.afterPropertiesSet();
        Assert.notNull(jdbcTemplate);
        if (fetchSize != VALUE_NOT_SET) {
            jdbcTemplate.setFetchSize(fetchSize);
        }
        jdbcTemplate.setMaxRows(getPageSize());
        Assert.notNull(queryProvider);
        this.firstPageSql = queryProvider.generateFirstPageQuery(getPageSize());
        this.remainingPagesSql = queryProvider.generateRemainingPagesQuery(getPageSize());
    }

    @Override
    protected T doRead() throws Exception {
        int current = currentInfo.get()==null?0:currentInfo.get();
        List<T> results = resultInfo.get();
        if (results == null || current >= getPageSize()) {
            synchronized (lock) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Reading page " + page);
                }
                if(page > 8) {
                    System.out.println("");
                }
                logger.info("Reading page " + page);
                doReadPage();
                current = currentInfo.get()==null?0:currentInfo.get();
                results = resultInfo.get();
                page++;
            }
            if (current >= getPageSize()) {
                current = 0;
            }
        }

        int next = current++;
        currentInfo.set(current);

        if (next < results.size()) {
            return results.get(next);
        }
        else {
            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void doReadPage() {
        List<T> results = resultInfo.get();
        if (results == null) {
            results = new CopyOnWriteArrayList<T>();
        } else {
            resultInfo.remove();
            results.clear();
        }

        PagingRowMapper rowCallback = new PagingRowMapper();

        List<?> query;

        if (page == 0) {
            if (logger.isDebugEnabled()) {
                logger.debug("SQL used for reading first page: [" + firstPageSql + "]");
            }
            if (parameterValues != null && parameterValues.size() > 0) {
                query = getJdbcTemplate().query(firstPageSql,
                        getParameterList(parameterValues, null).toArray(), rowCallback);
            }
            else {
                query = getJdbcTemplate().query(firstPageSql, rowCallback);
            }

        }
        else {
            previousStartAfterValues = startAfterValues;
            if (logger.isDebugEnabled()) {
                logger.debug("SQL used for reading remaining pages: [" + remainingPagesSql + "]" +":"+startAfterValues.get("user_id"));
            }
            query = getJdbcTemplate().query(remainingPagesSql,
                    getParameterList(parameterValues, startAfterValues).toArray(), rowCallback);
        }
        Collection<T> result = (Collection<T>) query;
        results.addAll(result);
        resultInfo.set(results);
        currentInfo.set(0);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        super.update(executionContext);
        if (isSaveState()) {
            if (isAtEndOfPage() && startAfterValues != null) {
                // restart on next page
                executionContext.put(getExecutionContextKey(START_AFTER_VALUE), startAfterValues);
            } else if (previousStartAfterValues != null) {
                // restart on current page
                executionContext.put(getExecutionContextKey(START_AFTER_VALUE), previousStartAfterValues);
            }
        }
    }

    private boolean isAtEndOfPage() {
        return getCurrentItemCount() % getPageSize() == 0;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void open(ExecutionContext executionContext) {
        if (isSaveState()) {
            startAfterValues = (Map<String, Object>) executionContext.get(getExecutionContextKey(START_AFTER_VALUE));

            if(startAfterValues == null) {
                startAfterValues = new LinkedHashMap<String, Object>();
            }
        }

        super.open(executionContext);
    }

    @Override
    protected void doJumpToPage(int itemIndex) {
		//TODO do nothing;
    }

    private Map<String, Object> getParameterMap(Map<String, Object> values, Map<String, Object> sortKeyValues) {
        Map<String, Object> parameterMap = new LinkedHashMap<String, Object>();
        if (values != null) {
            parameterMap.putAll(values);
        }
        if (sortKeyValues != null && !sortKeyValues.isEmpty()) {
            for (Map.Entry<String, Object> sortKey : sortKeyValues.entrySet()) {
                parameterMap.put("_" + sortKey.getKey(), sortKey.getValue());
            }
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Using parameterMap:" + parameterMap);
        }
        return parameterMap;
    }

    private List<Object> getParameterList(Map<String, Object> values, Map<String, Object> sortKeyValue) {
        SortedMap<String, Object> sm = new TreeMap<String, Object>();
        if (values != null) {
            sm.putAll(values);
        }
        List<Object> parameterList = new ArrayList<Object>();
        parameterList.addAll(sm.values());
        if (sortKeyValue != null && sortKeyValue.size() > 0) {
            List<Map.Entry<String, Object>> keys = new ArrayList<Map.Entry<String,Object>>(sortKeyValue.entrySet());

            for(int i = 0; i < keys.size(); i++) {
                for(int j = 0; j < i; j++) {
                    parameterList.add(keys.get(j).getValue());
                }

                parameterList.add(keys.get(i).getValue());
            }
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Using parameterList:" + parameterList);
        }
        return parameterList;
    }

    private class PagingRowMapper implements RowMapper<T> {
        @Override
        public T mapRow(ResultSet rs, int rowNum) throws SQLException {
            startAfterValues = new LinkedHashMap<String, Object>();
            for (Map.Entry<String, Order> sortKey : queryProvider.getSortKeys().entrySet()) {
                startAfterValues.put(sortKey.getKey(), rs.getObject(sortKey.getKey()));
            }

            return rowMapper.mapRow(rs, rowNum);
        }
    }

    public JdbcTemplate getJdbcTemplate() {
        return jdbcTemplate;
    }

}