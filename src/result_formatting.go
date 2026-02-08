package main

import (
	"encoding/hex"
	"fmt"
	"time"
	"strings"
	"errors"
	"strconv"
	"reflect"

	clickhouseDriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)



func formatResultValue(value any) string {
	if value == nil {
		return "NULL"
	}

	switch typed := value.(type) {
	case string:
		return typed
	case []byte:
		return hex.EncodeToString(typed)
	case time.Time:
		return typed.Format(time.RFC3339Nano)
	default:
		return fmt.Sprint(typed)
	}
}

func safeColumnTypes(rows clickhouseDriver.Rows) ([]string, error) {
	type columnTypesProvider interface {
		ColumnTypes() ([]clickhouseDriver.ColumnType, error)
	}

	provider, ok := any(rows).(columnTypesProvider)
	if !ok {
		return nil, errors.New("driver rows does not support ColumnTypes()")
	}

	columnTypes, err := provider.ColumnTypes()
	if err != nil {
		return nil, err
	}

	out := make([]string, 0, len(columnTypes))
	for _, ct := range columnTypes {
		out = append(out, ct.DatabaseTypeName())
	}
	return out, nil
}

func resolveDatabaseTypeNames(rows any) ([]string, error) {
	method := reflect.ValueOf(rows).MethodByName("ColumnTypes")
	if !method.IsValid() {
		return nil, fmt.Errorf("rows does not expose ColumnTypes()")
	}

	results := method.Call(nil)

	// Possible forms:
	//  - ColumnTypes() ([]T, error)
	//  - ColumnTypes() ([]T)
	if len(results) == 0 {
		return nil, fmt.Errorf("ColumnTypes() returned no values")
	}

	if len(results) == 2 {
		if !results[1].IsNil() {
			if err, ok := results[1].Interface().(error); ok {
				return nil, err
			}
			return nil, fmt.Errorf("ColumnTypes() returned a non-error second value")
		}
	}

	sliceValue := results[0]
	if sliceValue.Kind() != reflect.Slice {
		return nil, fmt.Errorf("ColumnTypes() first value is not a slice")
	}

	typeNames := make([]string, 0, sliceValue.Len())
	for index := 0; index < sliceValue.Len(); index++ {
		element := sliceValue.Index(index)

		// Try DatabaseTypeName() string
		dbTypeMethod := element.MethodByName("DatabaseTypeName")
		if dbTypeMethod.IsValid() {
			out := dbTypeMethod.Call(nil)
			if len(out) == 1 && out[0].Kind() == reflect.String {
				typeNames = append(typeNames, out[0].String())
				continue
			}
		}

		// Try Name() string (fallback)
		nameMethod := element.MethodByName("Name")
		if nameMethod.IsValid() {
			out := nameMethod.Call(nil)
			if len(out) == 1 && out[0].Kind() == reflect.String {
				typeNames = append(typeNames, out[0].String())
				continue
			}
		}

		// Last resort
		typeNames = append(typeNames, fmt.Sprint(element.Interface()))
	}

	return typeNames, nil
}

func allocateScanPointerForDatabaseType(databaseTypeName string) any {
	normalized := strings.ToLower(strings.TrimSpace(databaseTypeName))

	// Nullable(T)
	if strings.HasPrefix(normalized, "nullable(") && strings.HasSuffix(normalized, ")") {
		normalized = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(normalized, "nullable("), ")"))
	}

	// LowCardinality(T)
	if strings.HasPrefix(normalized, "lowcardinality(") && strings.HasSuffix(normalized, ")") {
		normalized = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(normalized, "lowcardinality("), ")"))
	}

	switch {
	case normalized == "string" || normalized == "uuid" || strings.HasPrefix(normalized, "fixedstring("):
		var v string
		return &v

	case normalized == "float32":
		var v float32
		return &v
	case normalized == "float64":
		var v float64
		return &v

	case normalized == "uint8":
		var v uint8
		return &v
	case normalized == "uint16":
		var v uint16
		return &v
	case normalized == "uint32":
		var v uint32
		return &v
	case normalized == "uint64":
		var v uint64
		return &v

	case normalized == "int8":
		var v int8
		return &v
	case normalized == "int16":
		var v int16
		return &v
	case normalized == "int32":
		var v int32
		return &v
	case normalized == "int64":
		var v int64
		return &v

	case normalized == "bool":
		var v bool
		return &v

	case normalized == "date" || normalized == "datetime" || strings.HasPrefix(normalized, "datetime64"):
		var v time.Time
		return &v

	default:
		// For now, represent unknown types as string ONLY if ClickHouse can scan it as string.
		// If you hit Arrays/Maps/Tuples/Decimals, we will add dedicated cases.
		var v string
		return &v
	}
}

func stringifyScanPointer(pointer any) string {
	switch v := pointer.(type) {
	case *string:
		if v == nil {
			return ""
		}
		return *v

	case *float32:
		return strconv.FormatFloat(float64(*v), 'f', -1, 32)
	case *float64:
		return strconv.FormatFloat(*v, 'f', -1, 64)

	case *uint8:
		return strconv.FormatUint(uint64(*v), 10)
	case *uint16:
		return strconv.FormatUint(uint64(*v), 10)
	case *uint32:
		return strconv.FormatUint(uint64(*v), 10)
	case *uint64:
		return strconv.FormatUint(*v, 10)

	case *int8:
		return strconv.FormatInt(int64(*v), 10)
	case *int16:
		return strconv.FormatInt(int64(*v), 10)
	case *int32:
		return strconv.FormatInt(int64(*v), 10)
	case *int64:
		return strconv.FormatInt(*v, 10)

	case *bool:
		if *v {
			return "true"
		}
		return "false"

	case *time.Time:
		return v.Format(time.RFC3339Nano)

	default:
		return fmt.Sprint(pointer)
	}
}

