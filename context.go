package dbo

import (
	"context"
	"gitlab.badanamu.com.cn/calmisland/common-log/log"
	"gorm.io/gorm"
)

// DBContext db with context
type DBContext struct {
	*gorm.DB
	ctx context.Context
}

// Print print sql log
func (s *DBContext) Printf(format string,v ...interface{}) {
	if len(v) != 5 {
		log.Debug(s.ctx, "invalid sql log", log.Any("args", v), log.String("logType", "sql"))
		return
	}

	// v[6]: ["sql", fileWithLineNum(), NowFunc().Sub(t), sql, vars, s.RowsAffected]
	log.Debug(s.ctx, v[0].(string),
		log.String("logType", "sql"),
		log.String("sql", v[4].(string)),
		log.Any("rowsAffected", v[3]),
		log.Float64("duration", v[2].(float64)))
}

// Clone create a new dbcontext without search condition
func (s *DBContext) Clone() *DBContext {
	return &DBContext{DB: s.DB.WithContext(s.ctx), ctx: s.ctx}
}
