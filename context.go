package dbo

import (
	"gitlab.badanamu.com.cn/calmisland/common-log/log"
	"gorm.io/gorm"
)

// DBContext db with context
type DBContext struct {
	*gorm.DB
}

// Print print sql log
func (s *DBContext) Printf(format string,v ...interface{}) {
	if len(v) != 5 {
		log.Debug(s.DB.Statement.Context, "invalid sql log", log.Any("args", v), log.String("logType", "sql"))
		return
	}
	// v[6]: ["sql", fileWithLineNum(), NowFunc().Sub(t), sql, vars, s.RowsAffected]
	log.Debug(s.Statement.Context, v[0].(string),
		log.String("logType", "sql"),
		log.String("sql", v[4].(string)),
		log.Any("rowsAffected", v[3]),
		log.Float64("duration", v[2].(float64)))
}

func (s *DBContext) getTableName(value interface{}) string{
	stmt := &gorm.Statement{DB: s.DB}
	err:=stmt.Parse(value)
	if err != nil{
		return ""
	}
	return stmt.Schema.Table

}