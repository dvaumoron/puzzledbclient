/*
 *
 * Copyright 2022 puzzledbclient authors.
 *
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
 *
 */
package puzzledbclient

import (
	"fmt"
	"os"
	"strings"

	"github.com/dvaumoron/puzzlelogger"
	"github.com/glebarez/sqlite" // driver without cgo
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gorm.io/driver/clickhouse"
	"gorm.io/driver/mysql"
	"gorm.io/driver/postgres"
	"gorm.io/driver/sqlserver"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

const gormKey = "gorm"

func Create(logger *zap.Logger) *gorm.DB {
	kind := strings.ToLower(os.Getenv("DB_SERVER_TYPE"))
	addr := os.Getenv("DB_SERVER_ADDR")
	var dialector gorm.Dialector
	switch kind {
	case "sqlite":
		dialector = sqlite.Open(addr)
	case "postgres":
		dialector = postgres.Open(addr)
	case "mysql":
		dialector = mysql.Open(addr)
	case "sqlserver":
		dialector = sqlserver.Open(addr)
	case "clickhouse":
		dialector = clickhouse.Open(addr)
	default:
		logger.Fatal("Unknown database type", zap.String("kind", kind))
	}

	db, err := gorm.Open(dialector, &gorm.Config{Logger: gormlogger.New(
		loggerWrapper{inner: logger}, gormlogger.Config{LogLevel: convertLevel(logger.Level())}),
	})
	if err != nil {
		logger.Fatal("Database connection failed", zap.Error(err))
	}
	return db
}

func Paginate(db *gorm.DB, start uint64, end uint64) *gorm.DB {
	return db.Offset(int(start)).Limit(int(end - start))
}

func BuildLikeFilter(filter string) string {
	filter = strings.ReplaceAll(filter, ".*", "%")
	var likeBuilder strings.Builder
	if strings.IndexByte(filter, '%') != 0 {
		likeBuilder.WriteByte('%')
	}
	likeBuilder.WriteString(filter)
	if strings.LastIndexByte(filter, '%') != len(filter)-1 {
		likeBuilder.WriteByte('%')
	}
	return likeBuilder.String()
}

type loggerWrapper struct {
	inner *zap.Logger
}

func (w loggerWrapper) Printf(msg string, args ...any) {
	fmt.Fprintf(puzzlelogger.InfoWrapper{Inner: w.inner, Lib: gormKey}, msg, args...)
}

func convertLevel(level zapcore.Level) gormlogger.LogLevel {
	switch level {
	case zapcore.DebugLevel, zapcore.InfoLevel:
		return gormlogger.Info
	case zapcore.WarnLevel:
		return gormlogger.Warn
	}
	return gormlogger.Error
}
