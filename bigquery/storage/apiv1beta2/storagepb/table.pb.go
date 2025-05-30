// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.35.2
// 	protoc        v4.25.7
// source: google/cloud/bigquery/storage/v1beta2/table.proto

package storagepb

import (
	_ "google.golang.org/genproto/googleapis/api/annotations"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type TableFieldSchema_Type int32

const (
	// Illegal value
	TableFieldSchema_TYPE_UNSPECIFIED TableFieldSchema_Type = 0
	// 64K, UTF8
	TableFieldSchema_STRING TableFieldSchema_Type = 1
	// 64-bit signed
	TableFieldSchema_INT64 TableFieldSchema_Type = 2
	// 64-bit IEEE floating point
	TableFieldSchema_DOUBLE TableFieldSchema_Type = 3
	// Aggregate type
	TableFieldSchema_STRUCT TableFieldSchema_Type = 4
	// 64K, Binary
	TableFieldSchema_BYTES TableFieldSchema_Type = 5
	// 2-valued
	TableFieldSchema_BOOL TableFieldSchema_Type = 6
	// 64-bit signed usec since UTC epoch
	TableFieldSchema_TIMESTAMP TableFieldSchema_Type = 7
	// Civil date - Year, Month, Day
	TableFieldSchema_DATE TableFieldSchema_Type = 8
	// Civil time - Hour, Minute, Second, Microseconds
	TableFieldSchema_TIME TableFieldSchema_Type = 9
	// Combination of civil date and civil time
	TableFieldSchema_DATETIME TableFieldSchema_Type = 10
	// Geography object
	TableFieldSchema_GEOGRAPHY TableFieldSchema_Type = 11
	// Numeric value
	TableFieldSchema_NUMERIC TableFieldSchema_Type = 12
	// BigNumeric value
	TableFieldSchema_BIGNUMERIC TableFieldSchema_Type = 13
	// Interval
	TableFieldSchema_INTERVAL TableFieldSchema_Type = 14
	// JSON, String
	TableFieldSchema_JSON TableFieldSchema_Type = 15
)

// Enum value maps for TableFieldSchema_Type.
var (
	TableFieldSchema_Type_name = map[int32]string{
		0:  "TYPE_UNSPECIFIED",
		1:  "STRING",
		2:  "INT64",
		3:  "DOUBLE",
		4:  "STRUCT",
		5:  "BYTES",
		6:  "BOOL",
		7:  "TIMESTAMP",
		8:  "DATE",
		9:  "TIME",
		10: "DATETIME",
		11: "GEOGRAPHY",
		12: "NUMERIC",
		13: "BIGNUMERIC",
		14: "INTERVAL",
		15: "JSON",
	}
	TableFieldSchema_Type_value = map[string]int32{
		"TYPE_UNSPECIFIED": 0,
		"STRING":           1,
		"INT64":            2,
		"DOUBLE":           3,
		"STRUCT":           4,
		"BYTES":            5,
		"BOOL":             6,
		"TIMESTAMP":        7,
		"DATE":             8,
		"TIME":             9,
		"DATETIME":         10,
		"GEOGRAPHY":        11,
		"NUMERIC":          12,
		"BIGNUMERIC":       13,
		"INTERVAL":         14,
		"JSON":             15,
	}
)

func (x TableFieldSchema_Type) Enum() *TableFieldSchema_Type {
	p := new(TableFieldSchema_Type)
	*p = x
	return p
}

func (x TableFieldSchema_Type) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (TableFieldSchema_Type) Descriptor() protoreflect.EnumDescriptor {
	return file_google_cloud_bigquery_storage_v1beta2_table_proto_enumTypes[0].Descriptor()
}

func (TableFieldSchema_Type) Type() protoreflect.EnumType {
	return &file_google_cloud_bigquery_storage_v1beta2_table_proto_enumTypes[0]
}

func (x TableFieldSchema_Type) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use TableFieldSchema_Type.Descriptor instead.
func (TableFieldSchema_Type) EnumDescriptor() ([]byte, []int) {
	return file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDescGZIP(), []int{1, 0}
}

type TableFieldSchema_Mode int32

const (
	// Illegal value
	TableFieldSchema_MODE_UNSPECIFIED TableFieldSchema_Mode = 0
	TableFieldSchema_NULLABLE         TableFieldSchema_Mode = 1
	TableFieldSchema_REQUIRED         TableFieldSchema_Mode = 2
	TableFieldSchema_REPEATED         TableFieldSchema_Mode = 3
)

// Enum value maps for TableFieldSchema_Mode.
var (
	TableFieldSchema_Mode_name = map[int32]string{
		0: "MODE_UNSPECIFIED",
		1: "NULLABLE",
		2: "REQUIRED",
		3: "REPEATED",
	}
	TableFieldSchema_Mode_value = map[string]int32{
		"MODE_UNSPECIFIED": 0,
		"NULLABLE":         1,
		"REQUIRED":         2,
		"REPEATED":         3,
	}
)

func (x TableFieldSchema_Mode) Enum() *TableFieldSchema_Mode {
	p := new(TableFieldSchema_Mode)
	*p = x
	return p
}

func (x TableFieldSchema_Mode) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (TableFieldSchema_Mode) Descriptor() protoreflect.EnumDescriptor {
	return file_google_cloud_bigquery_storage_v1beta2_table_proto_enumTypes[1].Descriptor()
}

func (TableFieldSchema_Mode) Type() protoreflect.EnumType {
	return &file_google_cloud_bigquery_storage_v1beta2_table_proto_enumTypes[1]
}

func (x TableFieldSchema_Mode) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use TableFieldSchema_Mode.Descriptor instead.
func (TableFieldSchema_Mode) EnumDescriptor() ([]byte, []int) {
	return file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDescGZIP(), []int{1, 1}
}

// Schema of a table
type TableSchema struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Describes the fields in a table.
	Fields []*TableFieldSchema `protobuf:"bytes,1,rep,name=fields,proto3" json:"fields,omitempty"`
}

func (x *TableSchema) Reset() {
	*x = TableSchema{}
	mi := &file_google_cloud_bigquery_storage_v1beta2_table_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TableSchema) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TableSchema) ProtoMessage() {}

func (x *TableSchema) ProtoReflect() protoreflect.Message {
	mi := &file_google_cloud_bigquery_storage_v1beta2_table_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TableSchema.ProtoReflect.Descriptor instead.
func (*TableSchema) Descriptor() ([]byte, []int) {
	return file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDescGZIP(), []int{0}
}

func (x *TableSchema) GetFields() []*TableFieldSchema {
	if x != nil {
		return x.Fields
	}
	return nil
}

// A field in TableSchema
type TableFieldSchema struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Required. The field name. The name must contain only letters (a-z, A-Z),
	// numbers (0-9), or underscores (_), and must start with a letter or
	// underscore. The maximum length is 128 characters.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Required. The field data type.
	Type TableFieldSchema_Type `protobuf:"varint,2,opt,name=type,proto3,enum=google.cloud.bigquery.storage.v1beta2.TableFieldSchema_Type" json:"type,omitempty"`
	// Optional. The field mode. The default value is NULLABLE.
	Mode TableFieldSchema_Mode `protobuf:"varint,3,opt,name=mode,proto3,enum=google.cloud.bigquery.storage.v1beta2.TableFieldSchema_Mode" json:"mode,omitempty"`
	// Optional. Describes the nested schema fields if the type property is set to STRUCT.
	Fields []*TableFieldSchema `protobuf:"bytes,4,rep,name=fields,proto3" json:"fields,omitempty"`
	// Optional. The field description. The maximum length is 1,024 characters.
	Description string `protobuf:"bytes,6,opt,name=description,proto3" json:"description,omitempty"`
}

func (x *TableFieldSchema) Reset() {
	*x = TableFieldSchema{}
	mi := &file_google_cloud_bigquery_storage_v1beta2_table_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *TableFieldSchema) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TableFieldSchema) ProtoMessage() {}

func (x *TableFieldSchema) ProtoReflect() protoreflect.Message {
	mi := &file_google_cloud_bigquery_storage_v1beta2_table_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TableFieldSchema.ProtoReflect.Descriptor instead.
func (*TableFieldSchema) Descriptor() ([]byte, []int) {
	return file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDescGZIP(), []int{1}
}

func (x *TableFieldSchema) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *TableFieldSchema) GetType() TableFieldSchema_Type {
	if x != nil {
		return x.Type
	}
	return TableFieldSchema_TYPE_UNSPECIFIED
}

func (x *TableFieldSchema) GetMode() TableFieldSchema_Mode {
	if x != nil {
		return x.Mode
	}
	return TableFieldSchema_MODE_UNSPECIFIED
}

func (x *TableFieldSchema) GetFields() []*TableFieldSchema {
	if x != nil {
		return x.Fields
	}
	return nil
}

func (x *TableFieldSchema) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

var File_google_cloud_bigquery_storage_v1beta2_table_proto protoreflect.FileDescriptor

var file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDesc = []byte{
	0x0a, 0x31, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2f, 0x62,
	0x69, 0x67, 0x71, 0x75, 0x65, 0x72, 0x79, 0x2f, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2f,
	0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x32, 0x2f, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x2e, 0x70, 0x72,
	0x6f, 0x74, 0x6f, 0x12, 0x25, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x63, 0x6c, 0x6f, 0x75,
	0x64, 0x2e, 0x62, 0x69, 0x67, 0x71, 0x75, 0x65, 0x72, 0x79, 0x2e, 0x73, 0x74, 0x6f, 0x72, 0x61,
	0x67, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x32, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x5f, 0x62, 0x65, 0x68,
	0x61, 0x76, 0x69, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x5e, 0x0a, 0x0b, 0x54,
	0x61, 0x62, 0x6c, 0x65, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x12, 0x4f, 0x0a, 0x06, 0x66, 0x69,
	0x65, 0x6c, 0x64, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x37, 0x2e, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x62, 0x69, 0x67, 0x71, 0x75, 0x65,
	0x72, 0x79, 0x2e, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74,
	0x61, 0x32, 0x2e, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x53, 0x63, 0x68,
	0x65, 0x6d, 0x61, 0x52, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x22, 0xf6, 0x04, 0x0a, 0x10,
	0x54, 0x61, 0x62, 0x6c, 0x65, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61,
	0x12, 0x17, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x42, 0x03,
	0xe0, 0x41, 0x02, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x55, 0x0a, 0x04, 0x74, 0x79, 0x70,
	0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x3c, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x62, 0x69, 0x67, 0x71, 0x75, 0x65, 0x72, 0x79, 0x2e,
	0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x32, 0x2e,
	0x54, 0x61, 0x62, 0x6c, 0x65, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61,
	0x2e, 0x54, 0x79, 0x70, 0x65, 0x42, 0x03, 0xe0, 0x41, 0x02, 0x52, 0x04, 0x74, 0x79, 0x70, 0x65,
	0x12, 0x55, 0x0a, 0x04, 0x6d, 0x6f, 0x64, 0x65, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x3c,
	0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x62, 0x69,
	0x67, 0x71, 0x75, 0x65, 0x72, 0x79, 0x2e, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2e, 0x76,
	0x31, 0x62, 0x65, 0x74, 0x61, 0x32, 0x2e, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x46, 0x69, 0x65, 0x6c,
	0x64, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x2e, 0x4d, 0x6f, 0x64, 0x65, 0x42, 0x03, 0xe0, 0x41,
	0x01, 0x52, 0x04, 0x6d, 0x6f, 0x64, 0x65, 0x12, 0x54, 0x0a, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64,
	0x73, 0x18, 0x04, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x37, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x62, 0x69, 0x67, 0x71, 0x75, 0x65, 0x72, 0x79, 0x2e,
	0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x32, 0x2e,
	0x54, 0x61, 0x62, 0x6c, 0x65, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61,
	0x42, 0x03, 0xe0, 0x41, 0x01, 0x52, 0x06, 0x66, 0x69, 0x65, 0x6c, 0x64, 0x73, 0x12, 0x25, 0x0a,
	0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x06, 0x20, 0x01,
	0x28, 0x09, 0x42, 0x03, 0xe0, 0x41, 0x01, 0x52, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70,
	0x74, 0x69, 0x6f, 0x6e, 0x22, 0xd5, 0x01, 0x0a, 0x04, 0x54, 0x79, 0x70, 0x65, 0x12, 0x14, 0x0a,
	0x10, 0x54, 0x59, 0x50, 0x45, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45,
	0x44, 0x10, 0x00, 0x12, 0x0a, 0x0a, 0x06, 0x53, 0x54, 0x52, 0x49, 0x4e, 0x47, 0x10, 0x01, 0x12,
	0x09, 0x0a, 0x05, 0x49, 0x4e, 0x54, 0x36, 0x34, 0x10, 0x02, 0x12, 0x0a, 0x0a, 0x06, 0x44, 0x4f,
	0x55, 0x42, 0x4c, 0x45, 0x10, 0x03, 0x12, 0x0a, 0x0a, 0x06, 0x53, 0x54, 0x52, 0x55, 0x43, 0x54,
	0x10, 0x04, 0x12, 0x09, 0x0a, 0x05, 0x42, 0x59, 0x54, 0x45, 0x53, 0x10, 0x05, 0x12, 0x08, 0x0a,
	0x04, 0x42, 0x4f, 0x4f, 0x4c, 0x10, 0x06, 0x12, 0x0d, 0x0a, 0x09, 0x54, 0x49, 0x4d, 0x45, 0x53,
	0x54, 0x41, 0x4d, 0x50, 0x10, 0x07, 0x12, 0x08, 0x0a, 0x04, 0x44, 0x41, 0x54, 0x45, 0x10, 0x08,
	0x12, 0x08, 0x0a, 0x04, 0x54, 0x49, 0x4d, 0x45, 0x10, 0x09, 0x12, 0x0c, 0x0a, 0x08, 0x44, 0x41,
	0x54, 0x45, 0x54, 0x49, 0x4d, 0x45, 0x10, 0x0a, 0x12, 0x0d, 0x0a, 0x09, 0x47, 0x45, 0x4f, 0x47,
	0x52, 0x41, 0x50, 0x48, 0x59, 0x10, 0x0b, 0x12, 0x0b, 0x0a, 0x07, 0x4e, 0x55, 0x4d, 0x45, 0x52,
	0x49, 0x43, 0x10, 0x0c, 0x12, 0x0e, 0x0a, 0x0a, 0x42, 0x49, 0x47, 0x4e, 0x55, 0x4d, 0x45, 0x52,
	0x49, 0x43, 0x10, 0x0d, 0x12, 0x0c, 0x0a, 0x08, 0x49, 0x4e, 0x54, 0x45, 0x52, 0x56, 0x41, 0x4c,
	0x10, 0x0e, 0x12, 0x08, 0x0a, 0x04, 0x4a, 0x53, 0x4f, 0x4e, 0x10, 0x0f, 0x22, 0x46, 0x0a, 0x04,
	0x4d, 0x6f, 0x64, 0x65, 0x12, 0x14, 0x0a, 0x10, 0x4d, 0x4f, 0x44, 0x45, 0x5f, 0x55, 0x4e, 0x53,
	0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x4e, 0x55,
	0x4c, 0x4c, 0x41, 0x42, 0x4c, 0x45, 0x10, 0x01, 0x12, 0x0c, 0x0a, 0x08, 0x52, 0x45, 0x51, 0x55,
	0x49, 0x52, 0x45, 0x44, 0x10, 0x02, 0x12, 0x0c, 0x0a, 0x08, 0x52, 0x45, 0x50, 0x45, 0x41, 0x54,
	0x45, 0x44, 0x10, 0x03, 0x42, 0x7e, 0x0a, 0x29, 0x63, 0x6f, 0x6d, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x62, 0x69, 0x67, 0x71, 0x75, 0x65, 0x72,
	0x79, 0x2e, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x2e, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61,
	0x32, 0x42, 0x0a, 0x54, 0x61, 0x62, 0x6c, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a,
	0x43, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x67, 0x6f, 0x2f, 0x62, 0x69, 0x67, 0x71, 0x75, 0x65, 0x72, 0x79, 0x2f, 0x73, 0x74,
	0x6f, 0x72, 0x61, 0x67, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x76, 0x31, 0x62, 0x65, 0x74, 0x61, 0x32,
	0x2f, 0x73, 0x74, 0x6f, 0x72, 0x61, 0x67, 0x65, 0x70, 0x62, 0x3b, 0x73, 0x74, 0x6f, 0x72, 0x61,
	0x67, 0x65, 0x70, 0x62, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDescOnce sync.Once
	file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDescData = file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDesc
)

func file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDescGZIP() []byte {
	file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDescOnce.Do(func() {
		file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDescData = protoimpl.X.CompressGZIP(file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDescData)
	})
	return file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDescData
}

var file_google_cloud_bigquery_storage_v1beta2_table_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_google_cloud_bigquery_storage_v1beta2_table_proto_msgTypes = make([]protoimpl.MessageInfo, 2)
var file_google_cloud_bigquery_storage_v1beta2_table_proto_goTypes = []any{
	(TableFieldSchema_Type)(0), // 0: google.cloud.bigquery.storage.v1beta2.TableFieldSchema.Type
	(TableFieldSchema_Mode)(0), // 1: google.cloud.bigquery.storage.v1beta2.TableFieldSchema.Mode
	(*TableSchema)(nil),        // 2: google.cloud.bigquery.storage.v1beta2.TableSchema
	(*TableFieldSchema)(nil),   // 3: google.cloud.bigquery.storage.v1beta2.TableFieldSchema
}
var file_google_cloud_bigquery_storage_v1beta2_table_proto_depIdxs = []int32{
	3, // 0: google.cloud.bigquery.storage.v1beta2.TableSchema.fields:type_name -> google.cloud.bigquery.storage.v1beta2.TableFieldSchema
	0, // 1: google.cloud.bigquery.storage.v1beta2.TableFieldSchema.type:type_name -> google.cloud.bigquery.storage.v1beta2.TableFieldSchema.Type
	1, // 2: google.cloud.bigquery.storage.v1beta2.TableFieldSchema.mode:type_name -> google.cloud.bigquery.storage.v1beta2.TableFieldSchema.Mode
	3, // 3: google.cloud.bigquery.storage.v1beta2.TableFieldSchema.fields:type_name -> google.cloud.bigquery.storage.v1beta2.TableFieldSchema
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_google_cloud_bigquery_storage_v1beta2_table_proto_init() }
func file_google_cloud_bigquery_storage_v1beta2_table_proto_init() {
	if File_google_cloud_bigquery_storage_v1beta2_table_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   2,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_google_cloud_bigquery_storage_v1beta2_table_proto_goTypes,
		DependencyIndexes: file_google_cloud_bigquery_storage_v1beta2_table_proto_depIdxs,
		EnumInfos:         file_google_cloud_bigquery_storage_v1beta2_table_proto_enumTypes,
		MessageInfos:      file_google_cloud_bigquery_storage_v1beta2_table_proto_msgTypes,
	}.Build()
	File_google_cloud_bigquery_storage_v1beta2_table_proto = out.File
	file_google_cloud_bigquery_storage_v1beta2_table_proto_rawDesc = nil
	file_google_cloud_bigquery_storage_v1beta2_table_proto_goTypes = nil
	file_google_cloud_bigquery_storage_v1beta2_table_proto_depIdxs = nil
}
