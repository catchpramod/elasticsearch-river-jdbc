
package org.xbib.elasticsearch.river.jdbc.strategy.simple;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import org.xbib.elasticsearch.gatherer.PlainIndexableObject;
import org.xbib.elasticsearch.gatherer.PlainKeyValueStreamListener;
import org.xbib.elasticsearch.river.jdbc.RiverMouth;
import org.xbib.elasticsearch.river.jdbc.RiverSource;
import org.xbib.elasticsearch.river.jdbc.strategy.mock.MockRiverMouth;
import org.xbib.elasticsearch.river.jdbc.support.AbstractRiverTest;
import org.xbib.elasticsearch.river.jdbc.support.RiverContext;
import org.xbib.elasticsearch.river.jdbc.support.RiverKeyValueStreamListener;
import org.xbib.elasticsearch.gatherer.IndexableObject;
import org.xbib.elasticsearch.gatherer.Values;
import org.xbib.io.keyvalue.KeyValueStreamListener;

import static org.elasticsearch.common.collect.Lists.newLinkedList;

public class SimpleRiverSourceTests extends AbstractRiverTest {

    private static final ESLogger logger = ESLoggerFactory.getLogger(SimpleRiverSourceTests.class.getName());

    @Override
    public RiverSource getRiverSource() {
        return new SimpleRiverSource();
    }

    @Override
    public RiverContext getRiverContext() {
        return new RiverContext();
    }

    @Test
    public void testConnectionClose() throws Exception {
        Connection connection = source.connectionForReading();
        assertFalse(connection.isClosed());
        source.closeReading();
        assertTrue(connection.isClosed());
        source.connectionForReading();
    }

    @Test
    @Parameters({"sql1"})
    public void testSQL(String sql) throws Exception {
        PreparedStatement statement = source.prepareQuery(sql);
        ResultSet results = source.executeQuery(statement);
        for (int i = 0; i < 5; i++) {
            assertTrue(results.next());
        }
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql2", "n"})
    public void testStarQuery(String sql, @Optional Integer n) throws Exception {
        List<? extends Object> params = newLinkedList();
        RiverMouth output = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                logger.debug("object={}", object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        KeyValueStreamListener listener = new RiverKeyValueStreamListener()
                .output(output);
        long rows = 0L;
        source.beforeRows(results, listener);
        while (source.nextRow(results, listener)) {
            rows++;
        }
        source.afterRows(results, listener);
        assertEquals(rows, n == null ? 5 : n);
        source.close(results);
        source.close(statement);
    }

    @Test
    @Parameters({"sql3"})
    public void testNullInteger(String sql) throws Exception {
        List<? extends Object> params = newLinkedList();
        RiverMouth mouth = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                if (object == null || object.source() == null) {
                    throw new IllegalArgumentException("object missing");
                }
                Values o = (Values)object.source().get("amount");
                if (o == null) {
                    o = (Values)object.source().get("AMOUNT"); // hsqldb is uppercase
                }
                else if (!o.isNull()) {
                    throw new IllegalArgumentException("amount not null??? " + o.getClass().getName() );
                }
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        KeyValueStreamListener listener = new RiverKeyValueStreamListener()
                .output(mouth);
        long rows = 0L;
        source.beforeRows(results, listener);
        if (source.nextRow(results, listener)) {
            // only one row
            rows++;
        }
        source.afterRows(results, listener);
        assertEquals(rows, 1);
        source.close(results);
        source.close(statement);
    }


    /**
     * Test JDBC Array to structured object array
     *
     * @param sql the array select statement
     * @throws Exception
     */
    @Test
    @Parameters({"sql4", "res1", "res2"})
    public void testArray(@Optional String sql, @Optional String res1, @Optional String res2) throws Exception {
        if (sql == null) {
            return;
        }
        List<? extends Object> params = newLinkedList();
        final List<IndexableObject> result = newLinkedList();
        RiverMouth mouth = new MockRiverMouth() {
            @Override
            public void index(IndexableObject object, boolean create) throws IOException {
                if (object == null || object.source() == null) {
                    throw new IllegalArgumentException("object missing");
                }
                result.add(object);
            }
        };
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        KeyValueStreamListener listener = new RiverKeyValueStreamListener()
                .output(mouth);
        long rows = 0L;
        source.beforeRows(results, listener);
        while (source.nextRow(results, listener)) {
            rows++;
        }
        source.afterRows(results, listener);
        assertEquals(rows, 2);
        source.close(results);
        source.close(statement);
        Iterator<IndexableObject> it = result.iterator();
        assertEquals(it.next().source().toString(), res1);
        assertEquals(it.next().source().toString(), res2);
    }

    @Test
    @Parameters({"sqlCw"})
    public void testExcerpt(String sql) throws Exception {
        List<? extends Object> params = new ArrayList();

        //String sql ="select e.id \"_id\",e.transcript_id \"transcript_id\",substring_index(e.class,\'.\',-1) \"type\",e.comment_text \"comment\",c.id \"code_id\",c.label \"code\",c.tree_id \"code_tree\",p.title \"project.title\",p.description \"project.description\",nvp.id \"project.attributes.[id]\",nvp.name \"project.attributes.[key]\",nvp.value \"project.attributes.[value]\",nvpp.id \"interview.attributes.[id]\",nvpp.name \"interview.attributes.[key]\",nvpp.value \"interview.attributes.[value]\",substr(t.transcript_text,e.start_position+1,(e.end_position-e.start_position)+1) \"exerpt\"from excerpt e left join code c on e.code_id=c.id left join transcript t on t.id=e.transcript_id left join interview i on i.id=t.interview_id left join interview_name_value_pair invp on i.id=invp.interview_interview_attributes_id left join name_value_pair nvp on invp.name_value_pair_id=nvp.id left join project p on p.id=i.project_id left join project_name_value_pair pnvp on p.id=pnvp.project_interview_attributes_id left join name_value_pair nvpp on pnvp.name_value_pair_id=nvpp.id";
        //    String sql="select product \"_id\",department \"order.fruits.[type]\",quantity \"order.fruits.[quantity]\", customer \"customer\", created \"date\" from orders";
        PreparedStatement statement = source.prepareQuery(sql);
        source.bind(statement, params);
        ResultSet results = source.executeQuery(statement);
        KeyValueStreamListener listener = new PlainKeyValueStreamListener().begin();
        long rows = 0L;
//        source.beforeFirstRow(results, listener);
        source.beforeRows(results, listener);
        while (source.nextRow(results, listener)) {
            rows++;
        }
//        listener.reset();
        assertEquals(rows,5); //total 5 rows in orders table..
        source.close(results);
        source.close(statement);
    }

}