// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
	"cloud.google.com/go/civil"
	"google.golang.org/genproto/googleapis/type/date"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// sqlTypeToPbType converts a bigtable.SQLType to its corresponding *btpb.Type.
func sqlTypeToPbType(sqlType bigtable.SQLType) (*btpb.Type, error) {
	if sqlType == nil {
		return nil, errors.New("bigtable: proxyTypeProto: SQLType cannot be nil")
	}

	switch sqlT := sqlType.(type) {
	case bigtable.BytesSQLType:
		return &btpb.Type{Kind: &btpb.Type_BytesType{BytesType: &btpb.Type_Bytes{}}}, nil
	case bigtable.StringSQLType:
		return &btpb.Type{Kind: &btpb.Type_StringType{StringType: &btpb.Type_String{}}}, nil
	case bigtable.Int64SQLType:
		return &btpb.Type{Kind: &btpb.Type_Int64Type{Int64Type: &btpb.Type_Int64{}}}, nil
	case bigtable.Float32SQLType:
		return &btpb.Type{Kind: &btpb.Type_Float32Type{Float32Type: &btpb.Type_Float32{}}}, nil
	case bigtable.Float64SQLType:
		return &btpb.Type{Kind: &btpb.Type_Float64Type{Float64Type: &btpb.Type_Float64{}}}, nil
	case bigtable.BoolSQLType:
		return &btpb.Type{Kind: &btpb.Type_BoolType{BoolType: &btpb.Type_Bool{}}}, nil
	case bigtable.TimestampSQLType:
		return &btpb.Type{Kind: &btpb.Type_TimestampType{TimestampType: &btpb.Type_Timestamp{}}}, nil
	case bigtable.DateSQLType:
		return &btpb.Type{Kind: &btpb.Type_DateType{DateType: &btpb.Type_Date{}}}, nil
	case bigtable.ArraySQLType:
		if sqlT.ElemType == nil {
			return nil, errors.New("bigtable: proxyTypeProto: ArraySQLType ElemType cannot be nil")
		}
		elemPbType, err := sqlTypeToPbType(sqlT.ElemType)
		if err != nil {
			return nil, fmt.Errorf("bigtable: proxyTypeProto: error processing Array ElemType: %w", err)
		}
		return &btpb.Type{Kind: &btpb.Type_ArrayType{ArrayType: &btpb.Type_Array{ElementType: elemPbType}}}, nil
	case bigtable.MapSQLType, bigtable.StructSQLType: // Explicitly list known unsupported types
		return nil, fmt.Errorf("bigtable: proxyTypeProto: unsupported SQLType: %T", sqlT)
	default: // Catch any other unknown types
		return nil, fmt.Errorf("bigtable: proxyTypeProto: unknown or unsupported SQLType: %T", sqlT)
	}
}

// pbTypeToSQLType converts a *btpb.Type to a bigtable.SQLType.
// This is a helper function, copied from the unexported version in bigtable/result_row.go
// as internal packages cannot directly access unexported functions from parent packages.
func pbTypeToSQLType(t *btpb.Type) (bigtable.SQLType, error) {
	if t == nil {
		return nil, errors.New("bigtable: input btpb.Type is nil")
	}
	switch k := t.Kind.(type) {
	case *btpb.Type_BytesType:
		return bigtable.BytesSQLType{}, nil
	case *btpb.Type_StringType:
		return bigtable.StringSQLType{}, nil
	case *btpb.Type_Int64Type:
		return bigtable.Int64SQLType{}, nil
	case *btpb.Type_Float32Type:
		return bigtable.Float32SQLType{}, nil
	case *btpb.Type_Float64Type:
		return bigtable.Float64SQLType{}, nil
	case *btpb.Type_BoolType:
		return bigtable.BoolSQLType{}, nil
	case *btpb.Type_TimestampType:
		return bigtable.TimestampSQLType{}, nil
	case *btpb.Type_DateType:
		return bigtable.DateSQLType{}, nil
	case *btpb.Type_ArrayType:
		elemTypeProto := k.ArrayType.GetElementType()
		if elemTypeProto == nil {
			return nil, errors.New("bigtable: array element type is nil")
		}
		elemSQLType, err := pbTypeToSQLType(elemTypeProto)
		if err != nil {
			return nil, fmt.Errorf("bigtable: could not determine array element SQLType: %w", err)
		}
		return bigtable.ArraySQLType{ElemType: elemSQLType}, nil
	case *btpb.Type_MapType:
		keyPbType := k.MapType.GetKeyType()
		valPbType := k.MapType.GetValueType()
		if keyPbType == nil || valPbType == nil {
			return nil, errors.New("map key or value type is nil")
		}
		keySQLType, err := pbTypeToSQLType(keyPbType)
		if err != nil {
			return nil, fmt.Errorf("invalid map key type: %w", err)
		}
		valueSQLType, err := pbTypeToSQLType(valPbType)
		if err != nil {
			return nil, fmt.Errorf("invalid map value type: %w", err)
		}
		return bigtable.MapSQLType{KeyType: keySQLType, ValueType: valueSQLType}, nil
	case *btpb.Type_StructType:
		fields := k.StructType.GetFields()
		structFields := make([]bigtable.StructSQLField, len(fields))
		for i, f := range fields {
			fieldPbType := f.GetType()
			if fieldPbType == nil {
				return nil, errors.New("struct field " + f.GetFieldName() + " type is nil")
			}
			fieldSQLType, err := pbTypeToSQLType(fieldPbType)
			if err != nil {
				return nil, fmt.Errorf("invalid struct field %q type: %w", f.GetFieldName(), err)
			}
			structFields[i] = bigtable.StructSQLField{Name: f.GetFieldName(), Type: fieldSQLType}
		}
		return bigtable.StructSQLType{Fields: structFields}, nil
	default:
		return nil, fmt.Errorf("bigtable: unsupported btpb.Type kind: %T", k)
	}
}

// pbValueToSQLType converts a protobuf Value to its SQLType representation.
func pbValueToSQLType(v *btpb.Value) (bigtable.SQLType, interface{}, error) {
	if v == nil {
		return nil, nil, errors.New("bigtable: input btpb.Value is nil")
	}
	if v.Kind == nil {
		if v.Type != nil {
			sqlType, err := pbTypeToSQLType(v.Type)
			if err != nil {
				return nil, nil, fmt.Errorf("bigtable: btpb.Value has nil Kind but failed to infer SQLType from Type: %w", err)
			}
			return sqlType, nil, nil // Typed NULL
		}
		return nil, nil, errors.New("bigtable: input btpb.Value has nil Kind and nil Type")
	}

	switch kind := v.Kind.(type) {
	case *btpb.Value_BytesValue:
		return bigtable.BytesSQLType{}, kind.BytesValue, nil
	case *btpb.Value_StringValue:
		return bigtable.StringSQLType{}, kind.StringValue, nil
	case *btpb.Value_IntValue:
		return bigtable.Int64SQLType{}, kind.IntValue, nil
	case *btpb.Value_FloatValue:
		if v.Type != nil {
			if _, ok := v.Type.Kind.(*btpb.Type_Float32Type); ok {
				return bigtable.Float32SQLType{}, float32(kind.FloatValue), nil
			}
		}
		return bigtable.Float64SQLType{}, kind.FloatValue, nil
	case *btpb.Value_BoolValue:
		return bigtable.BoolSQLType{}, kind.BoolValue, nil
	case *btpb.Value_TimestampValue:
		if kind.TimestampValue == nil {
			return bigtable.TimestampSQLType{}, nil, nil
		}
		return bigtable.TimestampSQLType{}, kind.TimestampValue.AsTime(), nil
	case *btpb.Value_DateValue:
		if kind.DateValue == nil {
			return bigtable.DateSQLType{}, nil, nil
		}
		return bigtable.DateSQLType{}, civil.Date{Year: int(kind.DateValue.Year), Month: time.Month(kind.DateValue.Month), Day: int(kind.DateValue.Day)}, nil
	case *btpb.Value_ArrayValue:
		elements := kind.ArrayValue.GetValues()
		goValues := make([]interface{}, len(elements))
		var elemSQLType bigtable.SQLType = bigtable.BytesSQLType{}

		if len(elements) > 0 {
			var firstElemType bigtable.SQLType
			var err error
			for i, elem := range elements {
				var currentElemSQLType bigtable.SQLType
				var val interface{}
				currentElemSQLType, val, err = pbValueToSQLType(elem)
				if err != nil {
					return nil, nil, fmt.Errorf("bigtable: error converting array element at index %d: %w", i, err)
				}
				goValues[i] = val
				if i == 0 {
					firstElemType = currentElemSQLType
				}
			}
			if firstElemType != nil {
				elemSQLType = firstElemType
			}
		}
		return bigtable.ArraySQLType{ElemType: elemSQLType}, goValues, nil
	default:
		return nil, nil, fmt.Errorf("bigtable: unsupported btpb.Value kind: %T", kind)
	}
}

// getDestForSQLType returns a pointer to a Go type suitable for scanning a value of the given bigtable.SQLType.
func getDestForSQLType(sqlType bigtable.SQLType) (interface{}, error) {
	if sqlType == nil {
		return nil, errors.New("bigtable: input SQLType cannot be nil")
	}

	switch sqlT := sqlType.(type) {
	case bigtable.BytesSQLType:
		return new([]byte), nil
	case bigtable.StringSQLType:
		return new(string), nil
	case bigtable.Int64SQLType:
		return new(int64), nil
	case bigtable.Float32SQLType:
		return new(float32), nil
	case bigtable.Float64SQLType:
		return new(float64), nil
	case bigtable.BoolSQLType:
		return new(bool), nil
	case bigtable.TimestampSQLType:
		return new(time.Time), nil
	case bigtable.DateSQLType:
		return new(civil.Date), nil
	case bigtable.ArraySQLType:
		if sqlT.ElemType == nil {
			return nil, errors.New("bigtable: ArraySQLType has nil ElemType")
		}
		switch elemT := sqlT.ElemType.(type) {
		case bigtable.BytesSQLType:
			return new([][]byte), nil
		case bigtable.StringSQLType:
			return new([]string), nil
		case bigtable.Int64SQLType:
			return new([]int64), nil
		case bigtable.Float32SQLType:
			return new([]float32), nil
		case bigtable.Float64SQLType:
			return new([]float64), nil
		case bigtable.BoolSQLType:
			return new([]bool), nil
		case bigtable.TimestampSQLType:
			return new([]time.Time), nil
		case bigtable.DateSQLType:
			return new([]civil.Date), nil
		case bigtable.ArraySQLType:
			return new([]interface{}), nil
		default:
			return nil, fmt.Errorf("bigtable: GetDestForSQLType unsupported ElemType for ArraySQLType: %T", elemT)
		}
	case bigtable.MapSQLType:
		return nil, fmt.Errorf("bigtable: GetDestForSQLType does not support direct scanning for MapSQLType")
	case bigtable.StructSQLType:
		return nil, fmt.Errorf("bigtable: GetDestForSQLType does not support direct scanning for StructSQLType")
	default:
		return nil, fmt.Errorf("bigtable: unknown or unsupported SQLType for GetDestForSQLType: %T", sqlT)
	}
}

// goValueToPbValue converts a Go value to its corresponding btpb.Value, based on the provided SQLType.
func goValueToPbValue(sqlType bigtable.SQLType, goValue interface{}) (*btpb.Value, error) {
	if sqlType == nil {
		return nil, errors.New("bigtable: goValueToPbValue: SQLType cannot be nil")
	}

	pbType, err := sqlTypeToPbType(sqlType) // Call the new/verified proxyTypeProto
	if err != nil {
		return nil, fmt.Errorf("bigtable: goValueToPbValue: failed to get pbType: %w", err)
	}

	// Per subtask instruction: For now, just return a basic &btpb.Value{Type: pbType}
	// The detailed value conversion will be done in a subsequent subtask.
	// This means the existing detailed switch logic is temporarily simplified.
	if goValue == nil { // Still useful to handle nil goValue specifically for TypeOnly
		return &btpb.Value{Type: pbType}, nil
	}

	// Placeholder for actual value encoding, for now, just return type.
	// Detailed switch will be re-added/verified in next step.
	// For this step, we are just ensuring proxyTypeProto is integrated.
	// return &btpb.Value{Type: pbType}, nil // Simplified return for this subtask - Now re-adding full logic

	// Handle general nil case first (after pbType is successfully obtained)
	if goValue == nil {
		return &btpb.Value{Type: pbType}, nil
	}

	switch sqlT := sqlType.(type) {
	case bigtable.BytesSQLType:
		typedVal, ok := goValue.([]byte)
		if !ok {
			expectedTypeNameStr := reflect.TypeOf(sqlT).Name()
			return nil, fmt.Errorf("bigtable: parameter type mismatch: expected Go type compatible with %s, but got %T", expectedTypeNameStr, goValue)
		}
		if typedVal == nil { // Explicit nil []byte is also a valid SQL NULL representation for Bytes
			return &btpb.Value{Type: pbType}, nil
		}
		return &btpb.Value{Type: pbType, Kind: &btpb.Value_BytesValue{BytesValue: typedVal}}, nil
	case bigtable.StringSQLType:
		typedVal, ok := goValue.(string)
		if !ok {
			expectedTypeNameStr := reflect.TypeOf(sqlT).Name()
			return nil, fmt.Errorf("bigtable: parameter type mismatch: expected Go type compatible with %s, but got %T", expectedTypeNameStr, goValue)
		}
		return &btpb.Value{Type: pbType, Kind: &btpb.Value_StringValue{StringValue: typedVal}}, nil
	case bigtable.Int64SQLType:
		reflectVal := reflect.ValueOf(goValue)
		int64ReflectType := reflect.TypeOf(int64(0))
		if reflectVal.IsValid() && reflectVal.Type().ConvertibleTo(int64ReflectType) {
			typedVal := reflectVal.Convert(int64ReflectType).Int()
			return &btpb.Value{Type: pbType, Kind: &btpb.Value_IntValue{IntValue: typedVal}}, nil
		}
		expectedTypeNameStr := reflect.TypeOf(sqlT).Name()
		return nil, fmt.Errorf("bigtable: parameter type mismatch: expected Go type compatible with %s, but got %T", expectedTypeNameStr, goValue)
	case bigtable.Float32SQLType:
		typedVal, ok := goValue.(float32)
		if !ok {
			expectedTypeNameStr := reflect.TypeOf(sqlT).Name()
			return nil, fmt.Errorf("bigtable: parameter type mismatch: expected Go type compatible with %s, but got %T", expectedTypeNameStr, goValue)
		}
		return &btpb.Value{Type: pbType, Kind: &btpb.Value_FloatValue{FloatValue: float64(typedVal)}}, nil
	case bigtable.Float64SQLType:
		typedVal, ok := goValue.(float64)
		if !ok {
			expectedTypeNameStr := reflect.TypeOf(sqlT).Name()
			return nil, fmt.Errorf("bigtable: parameter type mismatch: expected Go type compatible with %s, but got %T", expectedTypeNameStr, goValue)
		}
		return &btpb.Value{Type: pbType, Kind: &btpb.Value_FloatValue{FloatValue: typedVal}}, nil
	case bigtable.BoolSQLType:
		typedVal, ok := goValue.(bool)
		if !ok {
			expectedTypeNameStr := reflect.TypeOf(sqlT).Name()
			return nil, fmt.Errorf("bigtable: parameter type mismatch: expected Go type compatible with %s, but got %T", expectedTypeNameStr, goValue)
		}
		return &btpb.Value{Type: pbType, Kind: &btpb.Value_BoolValue{BoolValue: typedVal}}, nil
	case bigtable.TimestampSQLType:
		typedVal, ok := goValue.(time.Time)
		if !ok {
			expectedTypeNameStr := reflect.TypeOf(sqlT).Name()
			return nil, fmt.Errorf("bigtable: parameter type mismatch: expected Go type compatible with %s, but got %T", expectedTypeNameStr, goValue)
		}
		return &btpb.Value{Type: pbType, Kind: &btpb.Value_TimestampValue{TimestampValue: timestamppb.New(typedVal)}}, nil
	case bigtable.DateSQLType:
		typedVal, ok := goValue.(civil.Date)
		if !ok {
			expectedTypeNameStr := reflect.TypeOf(sqlT).Name()
			return nil, fmt.Errorf("bigtable: parameter type mismatch: expected Go type compatible with %s, but got %T", expectedTypeNameStr, goValue)
		}
		return &btpb.Value{Type: pbType, Kind: &btpb.Value_DateValue{DateValue: &date.Date{Year: int32(typedVal.Year), Month: int32(typedVal.Month), Day: int32(typedVal.Day)}}}, nil
	case bigtable.ArraySQLType:
		// The top-level `if goValue == nil` handles nil slices passed directly.
		valReflectValue := reflect.ValueOf(goValue)
		if valReflectValue.Kind() != reflect.Slice && valReflectValue.Kind() != reflect.Array {
			expectedTypeNameStr := fmt.Sprintf("ArraySQLType (elements: %s)", reflect.TypeOf(sqlT.ElemType).Name())
			return nil, fmt.Errorf("bigtable: parameter type mismatch: expected Go type compatible with %s, but got %T", expectedTypeNameStr, goValue)
		}

		pbValues := make([]*btpb.Value, 0, valReflectValue.Len())
		for i := 0; i < valReflectValue.Len(); i++ {
			elem := valReflectValue.Index(i).Interface()
			elemPbVal, err := goValueToPbValue(sqlT.ElemType, elem) // Recursive call
			if err != nil {
				// Construct specific error message for array element mismatch
				// The error from recursive call might already be a type mismatch error for the element
				if strings.Contains(err.Error(), "parameter type mismatch") {
					return nil, fmt.Errorf("bigtable: goValueToPbValue: error converting array element at index %d: %w", i, err)
				}
				// If it's another error (e.g. nil ElemType), wrap it generally
				return nil, fmt.Errorf("bigtable: goValueToPbValue: error processing array element at index %d: %w", i, err)

			}
			elemPbVal.Type = nil // Type shouldn't be set in nested Values
			pbValues = append(pbValues, elemPbVal)
		}
		return &btpb.Value{Type: pbType, Kind: &btpb.Value_ArrayValue{ArrayValue: &btpb.ArrayValue{Values: pbValues}}}, nil
	default:
		return nil, fmt.Errorf("bigtable: goValueToPbValue: unsupported SQLType: %T", sqlType)
	}
}
