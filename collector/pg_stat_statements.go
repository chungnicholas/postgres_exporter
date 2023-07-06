// Copyright 2023 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collector

import (
	"context"
	"database/sql"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
)

const statStatementsSubsystem = "stat_statements"

func init() {
	// WARNING:
	//   Disabled by default because this set of metrics can be quite expensive on a busy server
	//   Every unique query will cause a new timeseries to be created
	registerCollector(statStatementsSubsystem, defaultEnabled, NewPGStatStatementsCollector)
}

type PGStatStatementsCollector struct {
	log log.Logger
}

func NewPGStatStatementsCollector(config collectorConfig) (Collector, error) {
	return &PGStatStatementsCollector{log: config.logger}, nil
}

var (
	statSTatementsCallsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "calls_total"),
		"Number of times executed",
		[]string{"user", "datname", "query"},
		prometheus.Labels{},
	)
	statStatementsMeanSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "mean_seconds_total"),
		"Mean total time spent in the statement, in seconds",
		[]string{"user", "datname", "query"},
		prometheus.Labels{},
	)
	statStatementsMaxSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "max_seconds_total"),
		"Max total time spent in the statement, in seconds",
		[]string{"user", "datname", "query"},
		prometheus.Labels{},
	)
	statStatementsRowsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "rows_total"),
		"Total number of rows retrieved or affected by the statement",
		[]string{"user", "datname", "query"},
		prometheus.Labels{},
	)
	statStatementsBlockReadSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "block_read_seconds_total"),
		"Total time the statement spent reading blocks, in seconds",
		[]string{"user", "datname", "query"},
		prometheus.Labels{},
	)
	statStatementsBlockWriteSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "block_write_seconds_total"),
		"Total time the statement spent writing blocks, in seconds",
		[]string{"user", "datname", "query"},
		prometheus.Labels{},
	)

	// get statistics of all queries except pg_setting
	pgStatStatementsQuery = `SELECT
		pg_get_userbyid(userid) as user,
		pg_database.datname,
		pg_stat_statements.query,
		pg_stat_statements.calls as calls_total,
		pg_stat_statements.mean_plan_time + pg_stat_statements.mean_exec_time / 1000.0 as mean_seconds_total,
		pg_stat_statements.max_plan_time + pg_stat_statements.max_exec_time / 1000.0 as max_seconds_total,
		pg_stat_statements.rows as rows_total,
		pg_stat_statements.blk_read_time / 1000.0 as block_read_seconds_total,
		pg_stat_statements.blk_write_time / 1000.0 as block_write_seconds_total
	FROM pg_stat_statements
	JOIN pg_database
		ON pg_database.oid = pg_stat_statements.dbid
	WHERE pg_stat_statements.query NOT LIKE '%pg_setting%'
	;`

	pgStatStatementsReset = `SELECT 
		pg_stat_statements_reset()
	;`
)

type pgStatStatementsRow struct {
	user                   sql.NullString
	datname                sql.NullString
	query                  sql.NullString
	callsTotal             sql.NullInt64
	meanSecondsTotal       sql.NullFloat64
	maxSecondsTotal        sql.NullFloat64
	rowsTotal              sql.NullInt64
	blockReadSecondsTotal  sql.NullFloat64
	blockWriteSecondsTotal sql.NullFloat64
}

func (PGStatStatementsCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx, pgStatStatementsQuery)
	if err != nil {
		return err
	}
	var rowMetrics []pgStatStatementsRow

	// save metrics to array then reset statistics
	for rows.Next() {
		var user, datname, query sql.NullString
		var callsTotal, rowsTotal sql.NullInt64
		var meanSecondsTotal, maxSecondsTotal, blockReadSecondsTotal, blockWriteSecondsTotal sql.NullFloat64

		if err := rows.Scan(&user, &datname, &query, &callsTotal, &meanSecondsTotal, &maxSecondsTotal, &rowsTotal, &blockReadSecondsTotal, &blockWriteSecondsTotal); err != nil {
			return err
		}

		newPgStatementsRow := pgStatStatementsRow{
			user:                   user,
			datname:                datname,
			query:                  query,
			callsTotal:             callsTotal,
			meanSecondsTotal:       meanSecondsTotal,
			maxSecondsTotal:        maxSecondsTotal,
			rowsTotal:              rowsTotal,
			blockReadSecondsTotal:  blockReadSecondsTotal,
			blockWriteSecondsTotal: blockWriteSecondsTotal,
		}

		rowMetrics = append(rowMetrics, newPgStatementsRow)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	rows.Close()
	db.ExecContext(ctx, pgStatStatementsReset)

	// log metrics to prometheus
	for _, row := range rowMetrics {
		userLabel := "unknown"
		if row.user.Valid {
			userLabel = row.user.String
		}
		datnameLabel := "unknown"
		if row.datname.Valid {
			datnameLabel = row.datname.String
		}
		queryLabel := "unknown"
		if row.query.Valid {
			queryLabel = row.query.String
		}

		callsTotalMetric := 0.0
		if row.callsTotal.Valid {
			callsTotalMetric = float64(row.callsTotal.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statSTatementsCallsTotal,
			prometheus.CounterValue,
			callsTotalMetric,
			userLabel, datnameLabel, queryLabel,
		)

		meanSecondsTotalMetric := 0.0
		if row.meanSecondsTotal.Valid {
			meanSecondsTotalMetric = row.meanSecondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsMeanSecondsTotal,
			prometheus.CounterValue,
			meanSecondsTotalMetric,
			userLabel, datnameLabel, queryLabel,
		)

		maxSecondsTotalMetric := 0.0
		if row.maxSecondsTotal.Valid {
			maxSecondsTotalMetric = row.maxSecondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsMaxSecondsTotal,
			prometheus.CounterValue,
			maxSecondsTotalMetric,
			userLabel, datnameLabel, queryLabel,
		)

		rowsTotalMetric := 0.0
		if row.rowsTotal.Valid {
			rowsTotalMetric = float64(row.rowsTotal.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsRowsTotal,
			prometheus.CounterValue,
			rowsTotalMetric,
			userLabel, datnameLabel, queryLabel,
		)

		blockReadSecondsTotalMetric := 0.0
		if row.blockReadSecondsTotal.Valid {
			blockReadSecondsTotalMetric = row.blockReadSecondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsBlockReadSecondsTotal,
			prometheus.CounterValue,
			blockReadSecondsTotalMetric,
			userLabel, datnameLabel, queryLabel,
		)

		blockWriteSecondsTotalMetric := 0.0
		if row.blockWriteSecondsTotal.Valid {
			blockWriteSecondsTotalMetric = row.blockWriteSecondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsBlockWriteSecondsTotal,
			prometheus.CounterValue,
			blockWriteSecondsTotalMetric,
			userLabel, datnameLabel, queryLabel,
		)
	}

	return nil
}
