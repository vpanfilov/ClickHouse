#!/usr/bin/env bash
set -e

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
. $CURDIR/../shell_config.sh

TABLE_HASH="cityHash64(groupArray(cityHash64(*)))"

function pack_unpack_compare()
{
    local buf_file="${CLICKHOUSE_TMP}/test.buf.'.$3"

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.buf"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.buf_file"

    ${CLICKHOUSE_CLIENT} --query "CREATE TABLE test.buf ENGINE = Memory AS $1"
    local res_orig=$(${CLICKHOUSE_CLIENT} --max_threads=1 --query "SELECT $TABLE_HASH FROM test.buf")

    ${CLICKHOUSE_CLIENT} --max_threads=1 --query "CREATE TABLE test.buf_file ENGINE = File($3) AS SELECT * FROM test.buf"
    local res_db_file=$(${CLICKHOUSE_CLIENT} --max_threads=1 --query "SELECT $TABLE_HASH FROM test.buf_file")

    ${CLICKHOUSE_CLIENT} --max_threads=1 --query "SELECT * FROM test.buf FORMAT $3" > "$buf_file"
    local res_ch_local1=$(${CLICKHOUSE_LOCAL} --structure "$2" --file "$buf_file" --table "my super table" --input-format "$3" --output-format TabSeparated --query "SELECT $TABLE_HASH FROM \`my super table\`")
    local res_ch_local2=$(${CLICKHOUSE_LOCAL} --structure "$2" --table "my super table" --input-format "$3" --output-format TabSeparated --query "SELECT $TABLE_HASH FROM \`my super table\`" < "$buf_file")

    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.buf"
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS test.buf_file"
    rm -f "$buf_file" stderr

    echo $((res_orig - res_db_file)) $((res_orig - res_ch_local1)) $((res_orig - res_ch_local2))
}

pack_unpack_compare "SELECT number FROM system.numbers LIMIT 10000" "number UInt64" "TabSeparated"
pack_unpack_compare "SELECT number FROM system.numbers LIMIT 10000" "number UInt64" "Native"
pack_unpack_compare "SELECT number FROM system.numbers LIMIT 10000" "number UInt64" "JSONEachRow"
echo
pack_unpack_compare "SELECT name, is_aggregate FROM system.functions" "name String, is_aggregate UInt8" "TabSeparated"
pack_unpack_compare "SELECT name, is_aggregate FROM system.functions" "name String, is_aggregate UInt8" "Native"
pack_unpack_compare "SELECT name, is_aggregate FROM system.functions" "name String, is_aggregate UInt8" "TSKV"
echo
# Check settings are passed correctly
${CLICKHOUSE_LOCAL} --max_rows_in_distinct=33 -q "SELECT name, value FROM system.settings WHERE name = 'max_rows_in_distinct'"
${CLICKHOUSE_LOCAL} -q "SET max_rows_in_distinct=33; SELECT name, value FROM system.settings WHERE name = 'max_rows_in_distinct'"
${CLICKHOUSE_LOCAL} --max_bytes_before_external_group_by=1 --max_block_size=10 -q "SELECT sum(ignore(*)) FROM (SELECT number, count() FROM numbers(1000) GROUP BY number)"
echo
# Check exta options
(${CLICKHOUSE_LOCAL} --ignore-error --echo --silent -q "SELECT nothing_to_do();SELECT 42;" 2>&1 && echo "Wrong RC") || true
echo
${CLICKHOUSE_LOCAL} -q "CREATE TABLE sophisticated_default
(
    a UInt8 DEFAULT
    (
        SELECT number FROM system.numbers LIMIT 3,1
    ),
    b UInt8 ALIAS
    (
        SELECT dummy+9 FROM system.one
    ),
    c UInt8
) ENGINE = Memory; SELECT count() FROM system.tables WHERE name='sophisticated_default';"

# Help is not skipped
[[ `${CLICKHOUSE_LOCAL} --help | wc -l` > 100 ]]


if [ -t 0 ] ; then
    # this shell has a std-input, so we're not in batch mode

    # Check that help width is adaptive
    stty cols 99999
    rows1=`${CLICKHOUSE_LOCAL} --help | wc -l`
    stty cols 80
    rows2=`${CLICKHOUSE_LOCAL} --help | wc -l`
    [[ $rows1 < $rows2 ]]

    stty cols 99999
    rows1=`${CLICKHOUSE_CLIENT} --help | wc -l`
    stty cols 80
    rows2=`${CLICKHOUSE_CLIENT} --help | wc -l`
    [[ $rows1 < $rows2 ]]

    shopt -s checkwinsize || true
fi