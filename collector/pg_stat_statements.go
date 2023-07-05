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
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsMeanSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "mean_seconds_total"),
		"Mean total time spent in the statement, in seconds",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsMaxSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "max_seconds_total"),
		"Max total time spent in the statement, in seconds",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsRowsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "rows_total"),
		"Total number of rows retrieved or affected by the statement",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsBlockReadSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "block_read_seconds_total"),
		"Total time the statement spent reading blocks, in seconds",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)
	statStatementsBlockWriteSecondsTotal = prometheus.NewDesc(
		prometheus.BuildFQName(namespace, statStatementsSubsystem, "block_write_seconds_total"),
		"Total time the statement spent writing blocks, in seconds",
		[]string{"user", "datname", "queryid"},
		prometheus.Labels{},
	)

	pgStatStatementsQuery = `SELECT
		pg_get_userbyid(userid) as user,
		pg_database.datname,
		pg_stat_statements.queryid,
		pg_stat_statements.calls as calls_total,
		pg_stat_statements.mean_plan_time + pg_stat_statements.mean_exec_time / 1000.0 as mean_seconds_total,
		pg_stat_statements.max_plan_time + pg_stat_statements.max_exec_time / 1000.0 as max_seconds_total,
		pg_stat_statements.blk_read_time / 1000.0 as block_read_seconds_total,
		pg_stat_statements.blk_write_time / 1000.0 as block_write_seconds_total
	FROM pg_stat_statements
	JOIN pg_database
		ON pg_database.oid = pg_stat_statements.dbid
	;`
)

func (PGStatStatementsCollector) Update(ctx context.Context, instance *instance, ch chan<- prometheus.Metric) error {
	db := instance.getDB()
	rows, err := db.QueryContext(ctx,
		pgStatStatementsQuery)

	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var user, datname, queryid sql.NullString
		var callsTotal, rowsTotal sql.NullInt64
		var meanSecondsTotal, maxSecondsTotal, blockReadSecondsTotal, blockWriteSecondsTotal sql.NullFloat64

		if err := rows.Scan(&user, &datname, &queryid, &callsTotal, &meanSecondsTotal, &maxSecondsTotal, &rowsTotal, &blockReadSecondsTotal, &blockWriteSecondsTotal); err != nil {
			return err
		}

		userLabel := "unknown"
		if user.Valid {
			userLabel = user.String
		}
		datnameLabel := "unknown"
		if datname.Valid {
			datnameLabel = datname.String
		}
		queryidLabel := "unknown"
		if queryid.Valid {
			queryidLabel = queryid.String
		}

		callsTotalMetric := 0.0
		if callsTotal.Valid {
			callsTotalMetric = float64(callsTotal.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statSTatementsCallsTotal,
			prometheus.CounterValue,
			callsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		meanSecondsTotalMetric := 0.0
		if meanSecondsTotal.Valid {
			meanSecondsTotalMetric = meanSecondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsMeanSecondsTotal,
			prometheus.CounterValue,
			meanSecondsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		maxSecondsTotalMetric := 0.0
		if maxSecondsTotal.Valid {
			maxSecondsTotalMetric = maxSecondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsMaxSecondsTotal,
			prometheus.CounterValue,
			maxSecondsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		rowsTotalMetric := 0.0
		if rowsTotal.Valid {
			rowsTotalMetric = float64(rowsTotal.Int64)
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsRowsTotal,
			prometheus.CounterValue,
			rowsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		blockReadSecondsTotalMetric := 0.0
		if blockReadSecondsTotal.Valid {
			blockReadSecondsTotalMetric = blockReadSecondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsBlockReadSecondsTotal,
			prometheus.CounterValue,
			blockReadSecondsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)

		blockWriteSecondsTotalMetric := 0.0
		if blockWriteSecondsTotal.Valid {
			blockWriteSecondsTotalMetric = blockWriteSecondsTotal.Float64
		}
		ch <- prometheus.MustNewConstMetric(
			statStatementsBlockWriteSecondsTotal,
			prometheus.CounterValue,
			blockWriteSecondsTotalMetric,
			userLabel, datnameLabel, queryidLabel,
		)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}
