package io.imply.druid.query;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.druid.java.util.common.guava.Sequence;
import io.druid.java.util.common.guava.Sequences;
import io.druid.query.Druids;
import io.druid.query.Result;
import io.druid.query.filter.AndDimFilter;
import io.druid.query.filter.DimFilter;
import io.druid.query.filter.SelectorDimFilter;
import io.druid.query.select.EventHolder;
import io.druid.query.select.PagingSpec;
import io.druid.query.select.SelectQuery;
import io.druid.query.select.SelectResultValue;
import org.joda.time.Interval;

import java.util.List;

/**
 * 修改客户端后的测试,DruidClient存在问题
 * 已经在代码中注释出来
 * 该demo参考ExampleMain所写
 * Created by sunxin on 2018/3/15.
 */
public class TestOptionalOfClient {

    public static void main(String[] args) throws JsonProcessingException {


        DruidClient druidClient = DruidClient.create("192.168.101.50");

        final int threshold = 50;
        final SelectQuery selectQuery = Druids
                .newSelectQueryBuilder()
                .dataSource("test_thread_ALL")
                .intervals(ImmutableList.of(new Interval("2010-01-01/2018-03-16")))
                .filters(
                    new AndDimFilter(
                        ImmutableList.<DimFilter>of(
                            new SelectorDimFilter("isMemberSale", "1", null),
                            new SelectorDimFilter("brandKey", "1", null)
                        )
                    )
                )
                .dimensions(ImmutableList.of("saleQuantity", "totalAmount"))
                .pagingSpec(new PagingSpec(null, threshold))
                .build();

        // Fetch the results.
        final long startTime = System.currentTimeMillis();
        final Sequence<Result<SelectResultValue>> resultSequence = druidClient.execute(selectQuery);
        final List<Result<SelectResultValue>> resultList = Sequences.toList(
                resultSequence,
                Lists.<Result<SelectResultValue>>newArrayList()
        );
        final long fetchTime = System.currentTimeMillis() - startTime;

        // Print the results.
        int resultCount = 0;
        for (final Result<SelectResultValue> result : resultList) {
            for (EventHolder eventHolder : result.getValue().getEvents()) {
                System.out.println(eventHolder.getEvent());
                resultCount++;
            }
        }

        // Print statistics.
        System.out.println(
                String.format(
                        "Fetched %,d rows in %,dms.",
                        resultCount,
                        fetchTime
                )
        );
    }
}
