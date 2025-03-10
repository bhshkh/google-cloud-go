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

import btpb "cloud.google.com/go/bigtable/apiv2/bigtablepb"

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
		Type: &btpb.Type{
			Kind: &btpb.Type_StringType{
				StringType: &btpb.Type_String{},
			},
		},
		Kind: &btpb.Value_StringValue{
			StringValue: s.string,
		},
	}
}

func (s StringDataType) String() string {
	return s.string
}
