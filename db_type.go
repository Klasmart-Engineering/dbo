package dbo

type DBType string

const (
	MySQL         DBType = "mysql"
	NewRelicMySQL DBType = "newrelic_mysql"
)

func (t DBType) String() string {
	return string(t)
}

func (t DBType) DriverName() string {
	switch t {
	case MySQL:
		return "mysql"
	case NewRelicMySQL:
		return "nrmysql"
	default:
		return ""
	}
}
