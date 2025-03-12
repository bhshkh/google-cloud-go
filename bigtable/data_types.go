/*
Copyright 2024 Google LLC

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bigtable

import (
	"errors"
	"fmt"

	btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"
)

// DataType represents the type of data that is written to, read from, or stored
// in Bigtable. It is heavily based on the GoogleSQL standard to help maintain
// familiarity and consistency across products and features.
type DataType interface {
	typeProto() *btpb.Type
	dataProto() *btpb.Value
}

// StringType represents a string
type StringDataType struct {
	string string
}

// NewStringDataType creates StringDataType
func NewStringDataType(s string) StringDataType {
	return StringDataType{
		string: s,
	}
}

// Used while preparing the query
func (s StringDataType) typeProto() *btpb.Type {
	return &btpb.Type{
		Kind: &btpb.Type_StringType{
			StringType: &btpb.Type_String{},
		},
	}
}

// Used while binding parameters to prepared query
func (s StringDataType) dataProto() *btpb.Value {
	return &btpb.Value{
		Type: s.typeProto(),
		Kind: &btpb.Value_StringValue{
			StringValue: s.string,
		},
	}
}

func (s *StringDataType) String() string {
	return s.string
}

// StructDataType represents a struct
type StructDataType struct {
	fields map[string]DataType
}

// NewStructDataType creates StructDataType
func NewStructDataType(fields map[string]DataType) (*StructDataType, error) {
	if fields == nil {
		return nil, errors.New("fields map cannot be nil")
	}
	return &StructDataType{
		fields: fields,
	}, nil
}

// typeProto returns the protobuf representation of the struct type.
func (s *StructDataType) typeProto() *btpb.Type {
	fieldProtos := make([]*btpb.Type_Struct_Field, 0, len(s.fields))
	for name, fieldType := range s.fields {
		fieldProtos = append(fieldProtos, &btpb.Type_Struct_Field{
			FieldName: name,
			Type:      fieldType.typeProto(),
		})
	}

	return &btpb.Type{
		Kind: &btpb.Type_StructType{
			StructType: &btpb.Type_Struct{
				Fields: fieldProtos,
			},
		},
	}
}

// dataProto returns the protobuf representation of the struct data.
func (s *StructDataType) dataProto() *btpb.Value {
	valueProtos := make([]*btpb.Value, 0, len(s.fields))
	for _, fieldData := range s.fields {
		valueProtos = append(valueProtos, fieldData.dataProto())
	}

	return &btpb.Value{
		Type: s.typeProto(),
		Kind: &btpb.Value_ArrayValue{
			// ArrayValue: &btpb.Value_Array{
			// 	Values: valueProtos,
			// },
		},
	}
}

// GetField returns the value of a field by name.
func (s *StructDataType) GetField(name string) (DataType, error) {
	field, ok := s.fields[name]
	if !ok {
		return nil, fmt.Errorf("field %s not found in struct", name)
	}
	return field, nil
}

// SetField sets the value of a field by name.
func (s *StructDataType) SetField(name string, value DataType) error {
	if _, ok := s.fields[name]; !ok {
		return fmt.Errorf("field %s not found in struct", name)
	}
	s.fields[name] = value
	return nil
}

// Fields returns the fields of the struct.
func (s *StructDataType) Fields() map[string]DataType {
	return s.fields
}
