package dbo

import (
	"context"
	"time"

	"github.com/jinzhu/gorm"
	"gitlab.badanamu.com.cn/calmisland/common-cn/logger"
)

// DBContext db with context
type DBContext struct {
	*gorm.DB
	ctx context.Context
}

// Print print sql log
func (s *DBContext) Print(v ...interface{}) {
	if len(v) != 6 {
		logger.WithContext(s.ctx).
			WithField("logType", "sql").
			Debug(v...)
		return
	}

	// v[6]: ["sql", fileWithLineNum(), NowFunc().Sub(t), sql, vars, s.RowsAffected]
	logger.WithContext(s.ctx).
		WithField("logType", "sql").
		WithField("parameters", v[4]).
		WithField("rowsAffected", v[5]).
		WithField("duration", v[2].(time.Duration).String()).
		Debug(v[3])
}

// Clone create a new dbcontext without search condition
func (s *DBContext) Clone() *DBContext {
	return &DBContext{DB: s.New(), ctx: s.ctx}
}
