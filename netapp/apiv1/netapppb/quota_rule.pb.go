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
// source: google/cloud/netapp/v1/quota_rule.proto

package netapppb

import (
	_ "google.golang.org/genproto/googleapis/api/annotations"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	fieldmaskpb "google.golang.org/protobuf/types/known/fieldmaskpb"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

// Types of Quota Rule
type QuotaRule_Type int32

const (
	// Unspecified type for quota rule
	QuotaRule_TYPE_UNSPECIFIED QuotaRule_Type = 0
	// Individual user quota rule
	QuotaRule_INDIVIDUAL_USER_QUOTA QuotaRule_Type = 1
	// Individual group quota rule
	QuotaRule_INDIVIDUAL_GROUP_QUOTA QuotaRule_Type = 2
	// Default user quota rule
	QuotaRule_DEFAULT_USER_QUOTA QuotaRule_Type = 3
	// Default group quota rule
	QuotaRule_DEFAULT_GROUP_QUOTA QuotaRule_Type = 4
)

// Enum value maps for QuotaRule_Type.
var (
	QuotaRule_Type_name = map[int32]string{
		0: "TYPE_UNSPECIFIED",
		1: "INDIVIDUAL_USER_QUOTA",
		2: "INDIVIDUAL_GROUP_QUOTA",
		3: "DEFAULT_USER_QUOTA",
		4: "DEFAULT_GROUP_QUOTA",
	}
	QuotaRule_Type_value = map[string]int32{
		"TYPE_UNSPECIFIED":       0,
		"INDIVIDUAL_USER_QUOTA":  1,
		"INDIVIDUAL_GROUP_QUOTA": 2,
		"DEFAULT_USER_QUOTA":     3,
		"DEFAULT_GROUP_QUOTA":    4,
	}
)

func (x QuotaRule_Type) Enum() *QuotaRule_Type {
	p := new(QuotaRule_Type)
	*p = x
	return p
}

func (x QuotaRule_Type) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (QuotaRule_Type) Descriptor() protoreflect.EnumDescriptor {
	return file_google_cloud_netapp_v1_quota_rule_proto_enumTypes[0].Descriptor()
}

func (QuotaRule_Type) Type() protoreflect.EnumType {
	return &file_google_cloud_netapp_v1_quota_rule_proto_enumTypes[0]
}

func (x QuotaRule_Type) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use QuotaRule_Type.Descriptor instead.
func (QuotaRule_Type) EnumDescriptor() ([]byte, []int) {
	return file_google_cloud_netapp_v1_quota_rule_proto_rawDescGZIP(), []int{6, 0}
}

// Quota Rule states
type QuotaRule_State int32

const (
	// Unspecified state for quota rule
	QuotaRule_STATE_UNSPECIFIED QuotaRule_State = 0
	// Quota rule is creating
	QuotaRule_CREATING QuotaRule_State = 1
	// Quota rule is updating
	QuotaRule_UPDATING QuotaRule_State = 2
	// Quota rule is deleting
	QuotaRule_DELETING QuotaRule_State = 3
	// Quota rule is ready
	QuotaRule_READY QuotaRule_State = 4
	// Quota rule is in error state.
	QuotaRule_ERROR QuotaRule_State = 5
)

// Enum value maps for QuotaRule_State.
var (
	QuotaRule_State_name = map[int32]string{
		0: "STATE_UNSPECIFIED",
		1: "CREATING",
		2: "UPDATING",
		3: "DELETING",
		4: "READY",
		5: "ERROR",
	}
	QuotaRule_State_value = map[string]int32{
		"STATE_UNSPECIFIED": 0,
		"CREATING":          1,
		"UPDATING":          2,
		"DELETING":          3,
		"READY":             4,
		"ERROR":             5,
	}
)

func (x QuotaRule_State) Enum() *QuotaRule_State {
	p := new(QuotaRule_State)
	*p = x
	return p
}

func (x QuotaRule_State) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (QuotaRule_State) Descriptor() protoreflect.EnumDescriptor {
	return file_google_cloud_netapp_v1_quota_rule_proto_enumTypes[1].Descriptor()
}

func (QuotaRule_State) Type() protoreflect.EnumType {
	return &file_google_cloud_netapp_v1_quota_rule_proto_enumTypes[1]
}

func (x QuotaRule_State) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use QuotaRule_State.Descriptor instead.
func (QuotaRule_State) EnumDescriptor() ([]byte, []int) {
	return file_google_cloud_netapp_v1_quota_rule_proto_rawDescGZIP(), []int{6, 1}
}

// ListQuotaRulesRequest for listing quota rules.
type ListQuotaRulesRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Required. Parent value for ListQuotaRulesRequest
	Parent string `protobuf:"bytes,1,opt,name=parent,proto3" json:"parent,omitempty"`
	// Optional. Requested page size. Server may return fewer items than
	// requested. If unspecified, the server will pick an appropriate default.
	PageSize int32 `protobuf:"varint,2,opt,name=page_size,json=pageSize,proto3" json:"page_size,omitempty"`
	// Optional. A token identifying a page of results the server should return.
	PageToken string `protobuf:"bytes,3,opt,name=page_token,json=pageToken,proto3" json:"page_token,omitempty"`
	// Optional. Filtering results
	Filter string `protobuf:"bytes,4,opt,name=filter,proto3" json:"filter,omitempty"`
	// Optional. Hint for how to order the results
	OrderBy string `protobuf:"bytes,5,opt,name=order_by,json=orderBy,proto3" json:"order_by,omitempty"`
}

func (x *ListQuotaRulesRequest) Reset() {
	*x = ListQuotaRulesRequest{}
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[0]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListQuotaRulesRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListQuotaRulesRequest) ProtoMessage() {}

func (x *ListQuotaRulesRequest) ProtoReflect() protoreflect.Message {
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[0]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListQuotaRulesRequest.ProtoReflect.Descriptor instead.
func (*ListQuotaRulesRequest) Descriptor() ([]byte, []int) {
	return file_google_cloud_netapp_v1_quota_rule_proto_rawDescGZIP(), []int{0}
}

func (x *ListQuotaRulesRequest) GetParent() string {
	if x != nil {
		return x.Parent
	}
	return ""
}

func (x *ListQuotaRulesRequest) GetPageSize() int32 {
	if x != nil {
		return x.PageSize
	}
	return 0
}

func (x *ListQuotaRulesRequest) GetPageToken() string {
	if x != nil {
		return x.PageToken
	}
	return ""
}

func (x *ListQuotaRulesRequest) GetFilter() string {
	if x != nil {
		return x.Filter
	}
	return ""
}

func (x *ListQuotaRulesRequest) GetOrderBy() string {
	if x != nil {
		return x.OrderBy
	}
	return ""
}

// ListQuotaRulesResponse is the response to a ListQuotaRulesRequest.
type ListQuotaRulesResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// List of quota rules
	QuotaRules []*QuotaRule `protobuf:"bytes,1,rep,name=quota_rules,json=quotaRules,proto3" json:"quota_rules,omitempty"`
	// A token identifying a page of results the server should return.
	NextPageToken string `protobuf:"bytes,2,opt,name=next_page_token,json=nextPageToken,proto3" json:"next_page_token,omitempty"`
	// Locations that could not be reached.
	Unreachable []string `protobuf:"bytes,3,rep,name=unreachable,proto3" json:"unreachable,omitempty"`
}

func (x *ListQuotaRulesResponse) Reset() {
	*x = ListQuotaRulesResponse{}
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[1]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *ListQuotaRulesResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ListQuotaRulesResponse) ProtoMessage() {}

func (x *ListQuotaRulesResponse) ProtoReflect() protoreflect.Message {
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[1]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ListQuotaRulesResponse.ProtoReflect.Descriptor instead.
func (*ListQuotaRulesResponse) Descriptor() ([]byte, []int) {
	return file_google_cloud_netapp_v1_quota_rule_proto_rawDescGZIP(), []int{1}
}

func (x *ListQuotaRulesResponse) GetQuotaRules() []*QuotaRule {
	if x != nil {
		return x.QuotaRules
	}
	return nil
}

func (x *ListQuotaRulesResponse) GetNextPageToken() string {
	if x != nil {
		return x.NextPageToken
	}
	return ""
}

func (x *ListQuotaRulesResponse) GetUnreachable() []string {
	if x != nil {
		return x.Unreachable
	}
	return nil
}

// GetQuotaRuleRequest for getting a quota rule.
type GetQuotaRuleRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Required. Name of the quota rule
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (x *GetQuotaRuleRequest) Reset() {
	*x = GetQuotaRuleRequest{}
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[2]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *GetQuotaRuleRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*GetQuotaRuleRequest) ProtoMessage() {}

func (x *GetQuotaRuleRequest) ProtoReflect() protoreflect.Message {
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[2]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use GetQuotaRuleRequest.ProtoReflect.Descriptor instead.
func (*GetQuotaRuleRequest) Descriptor() ([]byte, []int) {
	return file_google_cloud_netapp_v1_quota_rule_proto_rawDescGZIP(), []int{2}
}

func (x *GetQuotaRuleRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

// CreateQuotaRuleRequest for creating a quota rule.
type CreateQuotaRuleRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Required. Parent value for CreateQuotaRuleRequest
	Parent string `protobuf:"bytes,1,opt,name=parent,proto3" json:"parent,omitempty"`
	// Required. Fields of the to be created quota rule.
	QuotaRule *QuotaRule `protobuf:"bytes,2,opt,name=quota_rule,json=quotaRule,proto3" json:"quota_rule,omitempty"`
	// Required. ID of the quota rule to create. Must be unique within the parent
	// resource. Must contain only letters, numbers, underscore and hyphen, with
	// the first character a letter or underscore, the last a letter or underscore
	// or a number, and a 63 character maximum.
	QuotaRuleId string `protobuf:"bytes,3,opt,name=quota_rule_id,json=quotaRuleId,proto3" json:"quota_rule_id,omitempty"`
}

func (x *CreateQuotaRuleRequest) Reset() {
	*x = CreateQuotaRuleRequest{}
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[3]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *CreateQuotaRuleRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*CreateQuotaRuleRequest) ProtoMessage() {}

func (x *CreateQuotaRuleRequest) ProtoReflect() protoreflect.Message {
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[3]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use CreateQuotaRuleRequest.ProtoReflect.Descriptor instead.
func (*CreateQuotaRuleRequest) Descriptor() ([]byte, []int) {
	return file_google_cloud_netapp_v1_quota_rule_proto_rawDescGZIP(), []int{3}
}

func (x *CreateQuotaRuleRequest) GetParent() string {
	if x != nil {
		return x.Parent
	}
	return ""
}

func (x *CreateQuotaRuleRequest) GetQuotaRule() *QuotaRule {
	if x != nil {
		return x.QuotaRule
	}
	return nil
}

func (x *CreateQuotaRuleRequest) GetQuotaRuleId() string {
	if x != nil {
		return x.QuotaRuleId
	}
	return ""
}

// UpdateQuotaRuleRequest for updating a quota rule.
type UpdateQuotaRuleRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Optional. Field mask is used to specify the fields to be overwritten in the
	// Quota Rule resource by the update.
	// The fields specified in the update_mask are relative to the resource, not
	// the full request. A field will be overwritten if it is in the mask. If the
	// user does not provide a mask then all fields will be overwritten.
	UpdateMask *fieldmaskpb.FieldMask `protobuf:"bytes,1,opt,name=update_mask,json=updateMask,proto3" json:"update_mask,omitempty"`
	// Required. The quota rule being updated
	QuotaRule *QuotaRule `protobuf:"bytes,2,opt,name=quota_rule,json=quotaRule,proto3" json:"quota_rule,omitempty"`
}

func (x *UpdateQuotaRuleRequest) Reset() {
	*x = UpdateQuotaRuleRequest{}
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[4]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *UpdateQuotaRuleRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*UpdateQuotaRuleRequest) ProtoMessage() {}

func (x *UpdateQuotaRuleRequest) ProtoReflect() protoreflect.Message {
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[4]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use UpdateQuotaRuleRequest.ProtoReflect.Descriptor instead.
func (*UpdateQuotaRuleRequest) Descriptor() ([]byte, []int) {
	return file_google_cloud_netapp_v1_quota_rule_proto_rawDescGZIP(), []int{4}
}

func (x *UpdateQuotaRuleRequest) GetUpdateMask() *fieldmaskpb.FieldMask {
	if x != nil {
		return x.UpdateMask
	}
	return nil
}

func (x *UpdateQuotaRuleRequest) GetQuotaRule() *QuotaRule {
	if x != nil {
		return x.QuotaRule
	}
	return nil
}

// DeleteQuotaRuleRequest for deleting a single quota rule.
type DeleteQuotaRuleRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Required. Name of the quota rule.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
}

func (x *DeleteQuotaRuleRequest) Reset() {
	*x = DeleteQuotaRuleRequest{}
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[5]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *DeleteQuotaRuleRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*DeleteQuotaRuleRequest) ProtoMessage() {}

func (x *DeleteQuotaRuleRequest) ProtoReflect() protoreflect.Message {
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[5]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use DeleteQuotaRuleRequest.ProtoReflect.Descriptor instead.
func (*DeleteQuotaRuleRequest) Descriptor() ([]byte, []int) {
	return file_google_cloud_netapp_v1_quota_rule_proto_rawDescGZIP(), []int{5}
}

func (x *DeleteQuotaRuleRequest) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

// QuotaRule specifies the maximum disk space a user or group can use within a
// volume. They can be used for creating default and individual quota rules.
type QuotaRule struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Identifier. The resource name of the quota rule.
	// Format:
	// `projects/{project_number}/locations/{location_id}/volumes/volumes/{volume_id}/quotaRules/{quota_rule_id}`.
	Name string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	// Optional. The quota rule applies to the specified user or group, identified
	// by a Unix UID/GID, Windows SID, or null for default.
	Target string `protobuf:"bytes,2,opt,name=target,proto3" json:"target,omitempty"`
	// Required. The type of quota rule.
	Type QuotaRule_Type `protobuf:"varint,3,opt,name=type,proto3,enum=google.cloud.netapp.v1.QuotaRule_Type" json:"type,omitempty"`
	// Required. The maximum allowed disk space in MiB.
	DiskLimitMib int32 `protobuf:"varint,4,opt,name=disk_limit_mib,json=diskLimitMib,proto3" json:"disk_limit_mib,omitempty"`
	// Output only. State of the quota rule
	State QuotaRule_State `protobuf:"varint,6,opt,name=state,proto3,enum=google.cloud.netapp.v1.QuotaRule_State" json:"state,omitempty"`
	// Output only. State details of the quota rule
	StateDetails string `protobuf:"bytes,7,opt,name=state_details,json=stateDetails,proto3" json:"state_details,omitempty"`
	// Output only. Create time of the quota rule
	CreateTime *timestamppb.Timestamp `protobuf:"bytes,8,opt,name=create_time,json=createTime,proto3" json:"create_time,omitempty"`
	// Optional. Description of the quota rule
	Description string `protobuf:"bytes,9,opt,name=description,proto3" json:"description,omitempty"`
	// Optional. Labels of the quota rule
	Labels map[string]string `protobuf:"bytes,10,rep,name=labels,proto3" json:"labels,omitempty" protobuf_key:"bytes,1,opt,name=key,proto3" protobuf_val:"bytes,2,opt,name=value,proto3"`
}

func (x *QuotaRule) Reset() {
	*x = QuotaRule{}
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[6]
	ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
	ms.StoreMessageInfo(mi)
}

func (x *QuotaRule) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QuotaRule) ProtoMessage() {}

func (x *QuotaRule) ProtoReflect() protoreflect.Message {
	mi := &file_google_cloud_netapp_v1_quota_rule_proto_msgTypes[6]
	if x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QuotaRule.ProtoReflect.Descriptor instead.
func (*QuotaRule) Descriptor() ([]byte, []int) {
	return file_google_cloud_netapp_v1_quota_rule_proto_rawDescGZIP(), []int{6}
}

func (x *QuotaRule) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *QuotaRule) GetTarget() string {
	if x != nil {
		return x.Target
	}
	return ""
}

func (x *QuotaRule) GetType() QuotaRule_Type {
	if x != nil {
		return x.Type
	}
	return QuotaRule_TYPE_UNSPECIFIED
}

func (x *QuotaRule) GetDiskLimitMib() int32 {
	if x != nil {
		return x.DiskLimitMib
	}
	return 0
}

func (x *QuotaRule) GetState() QuotaRule_State {
	if x != nil {
		return x.State
	}
	return QuotaRule_STATE_UNSPECIFIED
}

func (x *QuotaRule) GetStateDetails() string {
	if x != nil {
		return x.StateDetails
	}
	return ""
}

func (x *QuotaRule) GetCreateTime() *timestamppb.Timestamp {
	if x != nil {
		return x.CreateTime
	}
	return nil
}

func (x *QuotaRule) GetDescription() string {
	if x != nil {
		return x.Description
	}
	return ""
}

func (x *QuotaRule) GetLabels() map[string]string {
	if x != nil {
		return x.Labels
	}
	return nil
}

var File_google_cloud_netapp_v1_quota_rule_proto protoreflect.FileDescriptor

var file_google_cloud_netapp_v1_quota_rule_proto_rawDesc = []byte{
	0x0a, 0x27, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2f, 0x6e,
	0x65, 0x74, 0x61, 0x70, 0x70, 0x2f, 0x76, 0x31, 0x2f, 0x71, 0x75, 0x6f, 0x74, 0x61, 0x5f, 0x72,
	0x75, 0x6c, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x16, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x6e, 0x65, 0x74, 0x61, 0x70, 0x70, 0x2e, 0x76,
	0x31, 0x1a, 0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x66, 0x69,
	0x65, 0x6c, 0x64, 0x5f, 0x62, 0x65, 0x68, 0x61, 0x76, 0x69, 0x6f, 0x72, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x1a, 0x19, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x61, 0x70, 0x69, 0x2f, 0x72,
	0x65, 0x73, 0x6f, 0x75, 0x72, 0x63, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x20, 0x67,
	0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2f, 0x66,
	0x69, 0x65, 0x6c, 0x64, 0x5f, 0x6d, 0x61, 0x73, 0x6b, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a,
	0x1f, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2f, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66,
	0x2f, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x22, 0xdb, 0x01, 0x0a, 0x15, 0x4c, 0x69, 0x73, 0x74, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75,
	0x6c, 0x65, 0x73, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x3f, 0x0a, 0x06, 0x70, 0x61,
	0x72, 0x65, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x42, 0x27, 0xe0, 0x41, 0x02, 0xfa,
	0x41, 0x21, 0x12, 0x1f, 0x6e, 0x65, 0x74, 0x61, 0x70, 0x70, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52,
	0x75, 0x6c, 0x65, 0x52, 0x06, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x12, 0x20, 0x0a, 0x09, 0x70,
	0x61, 0x67, 0x65, 0x5f, 0x73, 0x69, 0x7a, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x05, 0x42, 0x03,
	0xe0, 0x41, 0x01, 0x52, 0x08, 0x70, 0x61, 0x67, 0x65, 0x53, 0x69, 0x7a, 0x65, 0x12, 0x22, 0x0a,
	0x0a, 0x70, 0x61, 0x67, 0x65, 0x5f, 0x74, 0x6f, 0x6b, 0x65, 0x6e, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x09, 0x42, 0x03, 0xe0, 0x41, 0x01, 0x52, 0x09, 0x70, 0x61, 0x67, 0x65, 0x54, 0x6f, 0x6b, 0x65,
	0x6e, 0x12, 0x1b, 0x0a, 0x06, 0x66, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x18, 0x04, 0x20, 0x01, 0x28,
	0x09, 0x42, 0x03, 0xe0, 0x41, 0x01, 0x52, 0x06, 0x66, 0x69, 0x6c, 0x74, 0x65, 0x72, 0x12, 0x1e,
	0x0a, 0x08, 0x6f, 0x72, 0x64, 0x65, 0x72, 0x5f, 0x62, 0x79, 0x18, 0x05, 0x20, 0x01, 0x28, 0x09,
	0x42, 0x03, 0xe0, 0x41, 0x01, 0x52, 0x07, 0x6f, 0x72, 0x64, 0x65, 0x72, 0x42, 0x79, 0x22, 0xa6,
	0x01, 0x0a, 0x16, 0x4c, 0x69, 0x73, 0x74, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65,
	0x73, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x42, 0x0a, 0x0b, 0x71, 0x75, 0x6f,
	0x74, 0x61, 0x5f, 0x72, 0x75, 0x6c, 0x65, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x21,
	0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x6e, 0x65,
	0x74, 0x61, 0x70, 0x70, 0x2e, 0x76, 0x31, 0x2e, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c,
	0x65, 0x52, 0x0a, 0x71, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x73, 0x12, 0x26, 0x0a,
	0x0f, 0x6e, 0x65, 0x78, 0x74, 0x5f, 0x70, 0x61, 0x67, 0x65, 0x5f, 0x74, 0x6f, 0x6b, 0x65, 0x6e,
	0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0d, 0x6e, 0x65, 0x78, 0x74, 0x50, 0x61, 0x67, 0x65,
	0x54, 0x6f, 0x6b, 0x65, 0x6e, 0x12, 0x20, 0x0a, 0x0b, 0x75, 0x6e, 0x72, 0x65, 0x61, 0x63, 0x68,
	0x61, 0x62, 0x6c, 0x65, 0x18, 0x03, 0x20, 0x03, 0x28, 0x09, 0x52, 0x0b, 0x75, 0x6e, 0x72, 0x65,
	0x61, 0x63, 0x68, 0x61, 0x62, 0x6c, 0x65, 0x22, 0x52, 0x0a, 0x13, 0x47, 0x65, 0x74, 0x51, 0x75,
	0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x3b,
	0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x42, 0x27, 0xe0, 0x41,
	0x02, 0xfa, 0x41, 0x21, 0x0a, 0x1f, 0x6e, 0x65, 0x74, 0x61, 0x70, 0x70, 0x2e, 0x67, 0x6f, 0x6f,
	0x67, 0x6c, 0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x51, 0x75, 0x6f, 0x74,
	0x61, 0x52, 0x75, 0x6c, 0x65, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x22, 0xc9, 0x01, 0x0a, 0x16,
	0x43, 0x72, 0x65, 0x61, 0x74, 0x65, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x52,
	0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x3f, 0x0a, 0x06, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74,
	0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x42, 0x27, 0xe0, 0x41, 0x02, 0xfa, 0x41, 0x21, 0x12, 0x1f,
	0x6e, 0x65, 0x74, 0x61, 0x70, 0x70, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x61, 0x70, 0x69,
	0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x52,
	0x06, 0x70, 0x61, 0x72, 0x65, 0x6e, 0x74, 0x12, 0x45, 0x0a, 0x0a, 0x71, 0x75, 0x6f, 0x74, 0x61,
	0x5f, 0x72, 0x75, 0x6c, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x21, 0x2e, 0x67, 0x6f,
	0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x6e, 0x65, 0x74, 0x61, 0x70,
	0x70, 0x2e, 0x76, 0x31, 0x2e, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x42, 0x03,
	0xe0, 0x41, 0x02, 0x52, 0x09, 0x71, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x12, 0x27,
	0x0a, 0x0d, 0x71, 0x75, 0x6f, 0x74, 0x61, 0x5f, 0x72, 0x75, 0x6c, 0x65, 0x5f, 0x69, 0x64, 0x18,
	0x03, 0x20, 0x01, 0x28, 0x09, 0x42, 0x03, 0xe0, 0x41, 0x02, 0x52, 0x0b, 0x71, 0x75, 0x6f, 0x74,
	0x61, 0x52, 0x75, 0x6c, 0x65, 0x49, 0x64, 0x22, 0xa1, 0x01, 0x0a, 0x16, 0x55, 0x70, 0x64, 0x61,
	0x74, 0x65, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65,
	0x73, 0x74, 0x12, 0x40, 0x0a, 0x0b, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x5f, 0x6d, 0x61, 0x73,
	0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65,
	0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x46, 0x69, 0x65, 0x6c, 0x64, 0x4d,
	0x61, 0x73, 0x6b, 0x42, 0x03, 0xe0, 0x41, 0x01, 0x52, 0x0a, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65,
	0x4d, 0x61, 0x73, 0x6b, 0x12, 0x45, 0x0a, 0x0a, 0x71, 0x75, 0x6f, 0x74, 0x61, 0x5f, 0x72, 0x75,
	0x6c, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x21, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c,
	0x65, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x6e, 0x65, 0x74, 0x61, 0x70, 0x70, 0x2e, 0x76,
	0x31, 0x2e, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x42, 0x03, 0xe0, 0x41, 0x02,
	0x52, 0x09, 0x71, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x22, 0x55, 0x0a, 0x16, 0x44,
	0x65, 0x6c, 0x65, 0x74, 0x65, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x52, 0x65,
	0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x3b, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x42, 0x27, 0xe0, 0x41, 0x02, 0xfa, 0x41, 0x21, 0x0a, 0x1f, 0x6e, 0x65, 0x74,
	0x61, 0x70, 0x70, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x61, 0x70, 0x69, 0x73, 0x2e, 0x63,
	0x6f, 0x6d, 0x2f, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x52, 0x04, 0x6e, 0x61,
	0x6d, 0x65, 0x22, 0x83, 0x07, 0x0a, 0x09, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65,
	0x12, 0x17, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x42, 0x03,
	0xe0, 0x41, 0x08, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1b, 0x0a, 0x06, 0x74, 0x61, 0x72,
	0x67, 0x65, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x42, 0x03, 0xe0, 0x41, 0x01, 0x52, 0x06,
	0x74, 0x61, 0x72, 0x67, 0x65, 0x74, 0x12, 0x3f, 0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x0e, 0x32, 0x26, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x63, 0x6c,
	0x6f, 0x75, 0x64, 0x2e, 0x6e, 0x65, 0x74, 0x61, 0x70, 0x70, 0x2e, 0x76, 0x31, 0x2e, 0x51, 0x75,
	0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x2e, 0x54, 0x79, 0x70, 0x65, 0x42, 0x03, 0xe0, 0x41,
	0x02, 0x52, 0x04, 0x74, 0x79, 0x70, 0x65, 0x12, 0x29, 0x0a, 0x0e, 0x64, 0x69, 0x73, 0x6b, 0x5f,
	0x6c, 0x69, 0x6d, 0x69, 0x74, 0x5f, 0x6d, 0x69, 0x62, 0x18, 0x04, 0x20, 0x01, 0x28, 0x05, 0x42,
	0x03, 0xe0, 0x41, 0x02, 0x52, 0x0c, 0x64, 0x69, 0x73, 0x6b, 0x4c, 0x69, 0x6d, 0x69, 0x74, 0x4d,
	0x69, 0x62, 0x12, 0x42, 0x0a, 0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x18, 0x06, 0x20, 0x01, 0x28,
	0x0e, 0x32, 0x27, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64,
	0x2e, 0x6e, 0x65, 0x74, 0x61, 0x70, 0x70, 0x2e, 0x76, 0x31, 0x2e, 0x51, 0x75, 0x6f, 0x74, 0x61,
	0x52, 0x75, 0x6c, 0x65, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x65, 0x42, 0x03, 0xe0, 0x41, 0x03, 0x52,
	0x05, 0x73, 0x74, 0x61, 0x74, 0x65, 0x12, 0x28, 0x0a, 0x0d, 0x73, 0x74, 0x61, 0x74, 0x65, 0x5f,
	0x64, 0x65, 0x74, 0x61, 0x69, 0x6c, 0x73, 0x18, 0x07, 0x20, 0x01, 0x28, 0x09, 0x42, 0x03, 0xe0,
	0x41, 0x03, 0x52, 0x0c, 0x73, 0x74, 0x61, 0x74, 0x65, 0x44, 0x65, 0x74, 0x61, 0x69, 0x6c, 0x73,
	0x12, 0x40, 0x0a, 0x0b, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x5f, 0x74, 0x69, 0x6d, 0x65, 0x18,
	0x08, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x62, 0x75, 0x66, 0x2e, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x42, 0x03, 0xe0, 0x41, 0x03, 0x52, 0x0a, 0x63, 0x72, 0x65, 0x61, 0x74, 0x65, 0x54, 0x69,
	0x6d, 0x65, 0x12, 0x25, 0x0a, 0x0b, 0x64, 0x65, 0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f,
	0x6e, 0x18, 0x09, 0x20, 0x01, 0x28, 0x09, 0x42, 0x03, 0xe0, 0x41, 0x01, 0x52, 0x0b, 0x64, 0x65,
	0x73, 0x63, 0x72, 0x69, 0x70, 0x74, 0x69, 0x6f, 0x6e, 0x12, 0x4a, 0x0a, 0x06, 0x6c, 0x61, 0x62,
	0x65, 0x6c, 0x73, 0x18, 0x0a, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x2d, 0x2e, 0x67, 0x6f, 0x6f, 0x67,
	0x6c, 0x65, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x6e, 0x65, 0x74, 0x61, 0x70, 0x70, 0x2e,
	0x76, 0x31, 0x2e, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x2e, 0x4c, 0x61, 0x62,
	0x65, 0x6c, 0x73, 0x45, 0x6e, 0x74, 0x72, 0x79, 0x42, 0x03, 0xe0, 0x41, 0x01, 0x52, 0x06, 0x6c,
	0x61, 0x62, 0x65, 0x6c, 0x73, 0x1a, 0x39, 0x0a, 0x0b, 0x4c, 0x61, 0x62, 0x65, 0x6c, 0x73, 0x45,
	0x6e, 0x74, 0x72, 0x79, 0x12, 0x10, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
	0x09, 0x52, 0x03, 0x6b, 0x65, 0x79, 0x12, 0x14, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x3a, 0x02, 0x38, 0x01,
	0x22, 0x84, 0x01, 0x0a, 0x04, 0x54, 0x79, 0x70, 0x65, 0x12, 0x14, 0x0a, 0x10, 0x54, 0x59, 0x50,
	0x45, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12,
	0x19, 0x0a, 0x15, 0x49, 0x4e, 0x44, 0x49, 0x56, 0x49, 0x44, 0x55, 0x41, 0x4c, 0x5f, 0x55, 0x53,
	0x45, 0x52, 0x5f, 0x51, 0x55, 0x4f, 0x54, 0x41, 0x10, 0x01, 0x12, 0x1a, 0x0a, 0x16, 0x49, 0x4e,
	0x44, 0x49, 0x56, 0x49, 0x44, 0x55, 0x41, 0x4c, 0x5f, 0x47, 0x52, 0x4f, 0x55, 0x50, 0x5f, 0x51,
	0x55, 0x4f, 0x54, 0x41, 0x10, 0x02, 0x12, 0x16, 0x0a, 0x12, 0x44, 0x45, 0x46, 0x41, 0x55, 0x4c,
	0x54, 0x5f, 0x55, 0x53, 0x45, 0x52, 0x5f, 0x51, 0x55, 0x4f, 0x54, 0x41, 0x10, 0x03, 0x12, 0x17,
	0x0a, 0x13, 0x44, 0x45, 0x46, 0x41, 0x55, 0x4c, 0x54, 0x5f, 0x47, 0x52, 0x4f, 0x55, 0x50, 0x5f,
	0x51, 0x55, 0x4f, 0x54, 0x41, 0x10, 0x04, 0x22, 0x5e, 0x0a, 0x05, 0x53, 0x74, 0x61, 0x74, 0x65,
	0x12, 0x15, 0x0a, 0x11, 0x53, 0x54, 0x41, 0x54, 0x45, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43,
	0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x43, 0x52, 0x45, 0x41, 0x54,
	0x49, 0x4e, 0x47, 0x10, 0x01, 0x12, 0x0c, 0x0a, 0x08, 0x55, 0x50, 0x44, 0x41, 0x54, 0x49, 0x4e,
	0x47, 0x10, 0x02, 0x12, 0x0c, 0x0a, 0x08, 0x44, 0x45, 0x4c, 0x45, 0x54, 0x49, 0x4e, 0x47, 0x10,
	0x03, 0x12, 0x09, 0x0a, 0x05, 0x52, 0x45, 0x41, 0x44, 0x59, 0x10, 0x04, 0x12, 0x09, 0x0a, 0x05,
	0x45, 0x52, 0x52, 0x4f, 0x52, 0x10, 0x05, 0x3a, 0x8e, 0x01, 0xea, 0x41, 0x8a, 0x01, 0x0a, 0x1f,
	0x6e, 0x65, 0x74, 0x61, 0x70, 0x70, 0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x61, 0x70, 0x69,
	0x73, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x12,
	0x50, 0x70, 0x72, 0x6f, 0x6a, 0x65, 0x63, 0x74, 0x73, 0x2f, 0x7b, 0x70, 0x72, 0x6f, 0x6a, 0x65,
	0x63, 0x74, 0x7d, 0x2f, 0x6c, 0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x73, 0x2f, 0x7b, 0x6c,
	0x6f, 0x63, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x7d, 0x2f, 0x76, 0x6f, 0x6c, 0x75, 0x6d, 0x65, 0x73,
	0x2f, 0x7b, 0x76, 0x6f, 0x6c, 0x75, 0x6d, 0x65, 0x7d, 0x2f, 0x71, 0x75, 0x6f, 0x74, 0x61, 0x52,
	0x75, 0x6c, 0x65, 0x73, 0x2f, 0x7b, 0x71, 0x75, 0x6f, 0x74, 0x61, 0x5f, 0x72, 0x75, 0x6c, 0x65,
	0x7d, 0x2a, 0x0a, 0x71, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x73, 0x32, 0x09, 0x71,
	0x75, 0x6f, 0x74, 0x61, 0x52, 0x75, 0x6c, 0x65, 0x42, 0xb0, 0x01, 0x0a, 0x1a, 0x63, 0x6f, 0x6d,
	0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x63, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x6e, 0x65,
	0x74, 0x61, 0x70, 0x70, 0x2e, 0x76, 0x31, 0x42, 0x0e, 0x51, 0x75, 0x6f, 0x74, 0x61, 0x52, 0x75,
	0x6c, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x50, 0x01, 0x5a, 0x32, 0x63, 0x6c, 0x6f, 0x75, 0x64,
	0x2e, 0x67, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x6f, 0x2f, 0x6e,
	0x65, 0x74, 0x61, 0x70, 0x70, 0x2f, 0x61, 0x70, 0x69, 0x76, 0x31, 0x2f, 0x6e, 0x65, 0x74, 0x61,
	0x70, 0x70, 0x70, 0x62, 0x3b, 0x6e, 0x65, 0x74, 0x61, 0x70, 0x70, 0x70, 0x62, 0xaa, 0x02, 0x16,
	0x47, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x2e, 0x43, 0x6c, 0x6f, 0x75, 0x64, 0x2e, 0x4e, 0x65, 0x74,
	0x41, 0x70, 0x70, 0x2e, 0x56, 0x31, 0xca, 0x02, 0x16, 0x47, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x5c,
	0x43, 0x6c, 0x6f, 0x75, 0x64, 0x5c, 0x4e, 0x65, 0x74, 0x41, 0x70, 0x70, 0x5c, 0x56, 0x31, 0xea,
	0x02, 0x19, 0x47, 0x6f, 0x6f, 0x67, 0x6c, 0x65, 0x3a, 0x3a, 0x43, 0x6c, 0x6f, 0x75, 0x64, 0x3a,
	0x3a, 0x4e, 0x65, 0x74, 0x41, 0x70, 0x70, 0x3a, 0x3a, 0x56, 0x31, 0x62, 0x06, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x33,
}

var (
	file_google_cloud_netapp_v1_quota_rule_proto_rawDescOnce sync.Once
	file_google_cloud_netapp_v1_quota_rule_proto_rawDescData = file_google_cloud_netapp_v1_quota_rule_proto_rawDesc
)

func file_google_cloud_netapp_v1_quota_rule_proto_rawDescGZIP() []byte {
	file_google_cloud_netapp_v1_quota_rule_proto_rawDescOnce.Do(func() {
		file_google_cloud_netapp_v1_quota_rule_proto_rawDescData = protoimpl.X.CompressGZIP(file_google_cloud_netapp_v1_quota_rule_proto_rawDescData)
	})
	return file_google_cloud_netapp_v1_quota_rule_proto_rawDescData
}

var file_google_cloud_netapp_v1_quota_rule_proto_enumTypes = make([]protoimpl.EnumInfo, 2)
var file_google_cloud_netapp_v1_quota_rule_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_google_cloud_netapp_v1_quota_rule_proto_goTypes = []any{
	(QuotaRule_Type)(0),            // 0: google.cloud.netapp.v1.QuotaRule.Type
	(QuotaRule_State)(0),           // 1: google.cloud.netapp.v1.QuotaRule.State
	(*ListQuotaRulesRequest)(nil),  // 2: google.cloud.netapp.v1.ListQuotaRulesRequest
	(*ListQuotaRulesResponse)(nil), // 3: google.cloud.netapp.v1.ListQuotaRulesResponse
	(*GetQuotaRuleRequest)(nil),    // 4: google.cloud.netapp.v1.GetQuotaRuleRequest
	(*CreateQuotaRuleRequest)(nil), // 5: google.cloud.netapp.v1.CreateQuotaRuleRequest
	(*UpdateQuotaRuleRequest)(nil), // 6: google.cloud.netapp.v1.UpdateQuotaRuleRequest
	(*DeleteQuotaRuleRequest)(nil), // 7: google.cloud.netapp.v1.DeleteQuotaRuleRequest
	(*QuotaRule)(nil),              // 8: google.cloud.netapp.v1.QuotaRule
	nil,                            // 9: google.cloud.netapp.v1.QuotaRule.LabelsEntry
	(*fieldmaskpb.FieldMask)(nil),  // 10: google.protobuf.FieldMask
	(*timestamppb.Timestamp)(nil),  // 11: google.protobuf.Timestamp
}
var file_google_cloud_netapp_v1_quota_rule_proto_depIdxs = []int32{
	8,  // 0: google.cloud.netapp.v1.ListQuotaRulesResponse.quota_rules:type_name -> google.cloud.netapp.v1.QuotaRule
	8,  // 1: google.cloud.netapp.v1.CreateQuotaRuleRequest.quota_rule:type_name -> google.cloud.netapp.v1.QuotaRule
	10, // 2: google.cloud.netapp.v1.UpdateQuotaRuleRequest.update_mask:type_name -> google.protobuf.FieldMask
	8,  // 3: google.cloud.netapp.v1.UpdateQuotaRuleRequest.quota_rule:type_name -> google.cloud.netapp.v1.QuotaRule
	0,  // 4: google.cloud.netapp.v1.QuotaRule.type:type_name -> google.cloud.netapp.v1.QuotaRule.Type
	1,  // 5: google.cloud.netapp.v1.QuotaRule.state:type_name -> google.cloud.netapp.v1.QuotaRule.State
	11, // 6: google.cloud.netapp.v1.QuotaRule.create_time:type_name -> google.protobuf.Timestamp
	9,  // 7: google.cloud.netapp.v1.QuotaRule.labels:type_name -> google.cloud.netapp.v1.QuotaRule.LabelsEntry
	8,  // [8:8] is the sub-list for method output_type
	8,  // [8:8] is the sub-list for method input_type
	8,  // [8:8] is the sub-list for extension type_name
	8,  // [8:8] is the sub-list for extension extendee
	0,  // [0:8] is the sub-list for field type_name
}

func init() { file_google_cloud_netapp_v1_quota_rule_proto_init() }
func file_google_cloud_netapp_v1_quota_rule_proto_init() {
	if File_google_cloud_netapp_v1_quota_rule_proto != nil {
		return
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_google_cloud_netapp_v1_quota_rule_proto_rawDesc,
			NumEnums:      2,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_google_cloud_netapp_v1_quota_rule_proto_goTypes,
		DependencyIndexes: file_google_cloud_netapp_v1_quota_rule_proto_depIdxs,
		EnumInfos:         file_google_cloud_netapp_v1_quota_rule_proto_enumTypes,
		MessageInfos:      file_google_cloud_netapp_v1_quota_rule_proto_msgTypes,
	}.Build()
	File_google_cloud_netapp_v1_quota_rule_proto = out.File
	file_google_cloud_netapp_v1_quota_rule_proto_rawDesc = nil
	file_google_cloud_netapp_v1_quota_rule_proto_goTypes = nil
	file_google_cloud_netapp_v1_quota_rule_proto_depIdxs = nil
}
