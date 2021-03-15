package dbo

import (
	"context"
	"time"

	"github.com/jinzhu/gorm"
	"gitlab.badanamu.com.cn/calmisland/common-log/log"
)

// DBContext db with context
type DBContext struct {
	*gorm.DB
	ctx context.Context
}

// Print print sql log
func (s *DBContext) Print(v ...interface{}) {
	if len(v) != 6 {
		log.Debug(s.ctx, "invalid sql log", log.Any("args", v), log.String("logType", "sql"))
		return
	}

	// v[6]: ["sql", fileWithLineNum(), NowFunc().Sub(t), sql, vars, s.RowsAffected]
	log.Debug(s.ctx, v[3].(string),
		log.String("logType", "sql"),
		log.Any("parameters", v[4]),
		log.Any("rowsAffected", v[5]),
		log.Int64("duration", v[2].(time.Duration).Milliseconds()))
}

// Clone create a new dbcontext without search condition
func (s *DBContext) Clone() *DBContext {
	return &DBContext{DB: s.New(), ctx: s.ctx}
}
