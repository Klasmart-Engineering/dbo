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
func (s *DBContext) Printf(format string, v ...interface{}) {
	if len(v) != 5 {
		log.Debug(s.DB.Statement.Context, "invalid sql log", log.Any("args", v), log.String("logType", "sql"))
		return
	}

	// v[5]: [fileWithLineNum(), "SLOW SQL >= 1µs, duration, rowAffected, sql]
	// example: ["/home/test/code.go:27","SLOW SQL >= 1µs",16.845856,1,"INSERT INTO `test_table` (`id`,`name`) VALUES ('1234','bad')"]
	log.Debug(s.Statement.Context, v[4].(string),
		log.String("logType", "sql"),
		log.String("lineNum", v[0].(string)),
		log.Any("rowsAffected", v[3]),
		log.Float64("duration", v[2].(float64)),
		log.Any("extra", v[1]))
}

// GetTableName get database table name of value
func (s *DBContext) GetTableName(value interface{}) string {
	stmt := &gorm.Statement{DB: s.DB}
	err := stmt.Parse(value)
	if err != nil {
		return ""
	}

	return stmt.Schema.Table
}

// ResetCondition reset session query conditions
func (s *DBContext) ResetCondition() *DBContext {
	s.DB = s.DB.Session(&gorm.Session{NewDB: true})
	return s
}
