/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.type;

import io.trino.sql.query.QueryAssertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.INDETERMINATE;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static io.trino.spi.function.OperatorType.SUBTRACT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle.PER_CLASS;

@TestInstance(PER_CLASS)
public class TestTime
{
    private QueryAssertions assertions;

    @BeforeAll
    public void init()
    {
        assertions = new QueryAssertions();
    }

    @AfterAll
    public void teardown()
    {
        assertions.close();
        assertions = null;
    }

    @Test
    public void testSubtract()
    {
        assertThat(assertions.operator(SUBTRACT, "TIME '14:15:16.432'", "TIME '03:04:05.321'"))
                .isEqualTo(new SqlIntervalDayTime(0, 11, 11, 11, 111));

        assertThat(assertions.operator(SUBTRACT, "TIME '03:04:05.321'", "TIME '14:15:16.432'"))
                .isEqualTo(new SqlIntervalDayTime(0, -11, -11, -11, -111));
    }

    @Test
    public void testEqual()
    {
        assertThat(assertions.operator(EQUAL, "TIME '03:04:05.321'", "TIME '03:04:05.321'")).isEqualTo(true);
        assertThat(assertions.operator(EQUAL, "TIME '03:04:05.321'", "TIME '03:04:05.333'")).isEqualTo(false);
    }

    @Test
    public void testNotEqual()
    {
        assertThat(assertions.expression("TIME '03:04:05.321' <> TIME '03:04:05.333'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:04:05.321' <> TIME '03:04:05.321'")).isEqualTo(false);
    }

    @Test
    public void testLessThan()
    {
        assertThat(assertions.operator(LESS_THAN, "TIME '03:04:05.321'", "TIME '03:04:05.333'")).isEqualTo(true);
        assertThat(assertions.operator(LESS_THAN, "TIME '03:04:05.321'", "TIME '03:04:05.321'")).isEqualTo(false);
        assertThat(assertions.operator(LESS_THAN, "TIME '03:04:05.321'", "TIME '03:04:05'")).isEqualTo(false);
    }

    @Test
    public void testLessThanOrEqual()
    {
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIME '03:04:05.321'", "TIME '03:04:05.333'")).isEqualTo(true);
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIME '03:04:05.321'", "TIME '03:04:05.321'")).isEqualTo(true);
        assertThat(assertions.operator(LESS_THAN_OR_EQUAL, "TIME '03:04:05.321'", "TIME '03:04:05'")).isEqualTo(false);
    }

    @Test
    public void testGreaterThan()
    {
        assertThat(assertions.expression("TIME '03:04:05.321' > TIME '03:04:05.111'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:04:05.321' > TIME '03:04:05.321'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:04:05.321' > TIME '03:04:05.333'")).isEqualTo(false);
    }

    @Test
    public void testGreaterThanOrEqual()
    {
        assertThat(assertions.expression("TIME '03:04:05.321' >= TIME '03:04:05.111'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:04:05.321' >= TIME '03:04:05.321'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:04:05.321' >= TIME '03:04:05.333'")).isEqualTo(false);
    }

    @Test
    public void testBetween()
    {
        assertThat(assertions.expression("TIME '03:04:05.321' between TIME '03:04:05.111' and TIME '03:04:05.333'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:04:05.321' between TIME '03:04:05.321' and TIME '03:04:05.333'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:04:05.321' between TIME '03:04:05.111' and TIME '03:04:05.321'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:04:05.321' between TIME '03:04:05.321' and TIME '03:04:05.321'")).isEqualTo(true);
        assertThat(assertions.expression("TIME '03:04:05.321' between TIME '03:04:05.322' and TIME '03:04:05.333'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:04:05.321' between TIME '03:04:05.311' and TIME '03:04:05.312'")).isEqualTo(false);
        assertThat(assertions.expression("TIME '03:04:05.321' between TIME '03:04:05.333' and TIME '03:04:05.111'")).isEqualTo(false);
    }

    @Test
    public void testCastToVarchar()
    {
        assertThat(assertions.expression("cast(TIME '03:04:05.321' as varchar)")).isEqualTo("03:04:05.321");
        assertThat(assertions.expression("cast(TIME '03:04:05' as varchar)")).isEqualTo("03:04:05");
        assertThat(assertions.expression("cast(TIME '03:04' as varchar)")).isEqualTo("03:04:00");
    }

    @Test
    public void testCastFromVarchar()
    {
        assertThat(assertions.operator(EQUAL, "cast('03:04:05.321' as time)", "TIME '03:04:05.321'")).isEqualTo(true);
        assertThat(assertions.operator(EQUAL, "cast('03:04:05' as time)", "TIME '03:04:05.000'")).isEqualTo(true);
        assertThat(assertions.operator(EQUAL, "cast('03:04' as time)", "TIME '03:04:00.000'")).isEqualTo(true);
    }

    @Test
    public void testIndeterminate()
    {
        assertThat(assertions.operator(INDETERMINATE, "cast(null as TIME)")).isEqualTo(true);
        assertThat(assertions.operator(INDETERMINATE, "TIME '00:00:00'")).isEqualTo(false);
    }
}
