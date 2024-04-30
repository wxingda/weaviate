// Code generated by protoc-gen-go. DO NOT EDIT.

package protocol

import (
	reflect "reflect"
	sync "sync"

	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type TenantActivityStatus int32

const (
	TenantActivityStatus_TENANT_ACTIVITY_STATUS_UNSPECIFIED TenantActivityStatus = 0
	TenantActivityStatus_TENANT_ACTIVITY_STATUS_HOT         TenantActivityStatus = 1
	TenantActivityStatus_TENANT_ACTIVITY_STATUS_COLD        TenantActivityStatus = 2
	TenantActivityStatus_TENANT_ACTIVITY_STATUS_WARM        TenantActivityStatus = 3
	TenantActivityStatus_TENANT_ACTIVITY_STATUS_FROZEN      TenantActivityStatus = 4
)

// Enum value maps for TenantActivityStatus.
var (
	TenantActivityStatus_name = map[int32]string{
		0: "TENANT_ACTIVITY_STATUS_UNSPECIFIED",
		1: "TENANT_ACTIVITY_STATUS_HOT",
		2: "TENANT_ACTIVITY_STATUS_COLD",
		3: "TENANT_ACTIVITY_STATUS_WARM",
		4: "TENANT_ACTIVITY_STATUS_FROZEN",
	}
	TenantActivityStatus_value = map[string]int32{
		"TENANT_ACTIVITY_STATUS_UNSPECIFIED": 0,
		"TENANT_ACTIVITY_STATUS_HOT":         1,
		"TENANT_ACTIVITY_STATUS_COLD":        2,
		"TENANT_ACTIVITY_STATUS_WARM":        3,
		"TENANT_ACTIVITY_STATUS_FROZEN":      4,
	}
)

func (x TenantActivityStatus) Enum() *TenantActivityStatus {
	p := new(TenantActivityStatus)
	*p = x
	return p
}

func (x TenantActivityStatus) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (TenantActivityStatus) Descriptor() protoreflect.EnumDescriptor {
	return file_v1_tenants_proto_enumTypes[0].Descriptor()
}

func (TenantActivityStatus) Type() protoreflect.EnumType {
	return &file_v1_tenants_proto_enumTypes[0]
}

func (x TenantActivityStatus) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use TenantActivityStatus.Descriptor instead.
func (TenantActivityStatus) EnumDescriptor() ([]byte, []int) {
	return file_v1_tenants_proto_rawDescGZIP(), []int{0}
}

type TenantsGetRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Collection       string            `protobuf:"bytes,1,opt,name=collection,proto3" json:"collection,omitempty"`
	ConsistencyLevel *ConsistencyLevel `protobuf:"varint,2,opt,name=consistency_level,json=consistencyLevel,proto3,enum=weaviate.v1.ConsistencyLevel,oneof" json:"consistency_level,omitempty"`
	// we might need to add a tenant-cursor api at some point, make this easily extendable
	//
	// Types that are assignable to Params:
	//
	//	*TenantsGetRequest_Names
	Params isTenantsGetRequest_Params `protobuf_oneof:"params"`
}

func (x *TenantsGetRequest) Reset() {
	*x = TenantsGetRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_v1_tenants_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TenantsGetRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TenantsGetRequest) ProtoMessage() {}

func (x *TenantsGetRequest) ProtoReflect() protoreflect.Message {
	mi := &file_v1_tenants_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TenantsGetRequest.ProtoReflect.Descriptor instead.
func (*TenantsGetRequest) Descriptor() ([]byte, []int) {
	return file_v1_tenants_proto_rawDescGZIP(), []int{0}
}

func (x *TenantsGetRequest) GetCollection() string {
	if x != nil {
		return x.Collection
	}
	return ""
}

func (x *TenantsGetRequest) GetConsistencyLevel() ConsistencyLevel {
	if x != nil && x.ConsistencyLevel != nil {
		return *x.ConsistencyLevel
	}
	return ConsistencyLevel_CONSISTENCY_LEVEL_UNSPECIFIED
}

func (m *TenantsGetRequest) GetParams() isTenantsGetRequest_Params {
	if m != nil {
		return m.Params
	}
	return nil
}

func (x *TenantsGetRequest) GetNames() *TenantNames {
	if x, ok := x.GetParams().(*TenantsGetRequest_Names); ok {
		return x.Names
	}
	return nil
}

type isTenantsGetRequest_Params interface {
	isTenantsGetRequest_Params()
}

type TenantsGetRequest_Names struct {
	Names *TenantNames `protobuf:"bytes,3,opt,name=names,proto3,oneof"`
}

func (*TenantsGetRequest_Names) isTenantsGetRequest_Params() {}

type TenantNames struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Values []string `protobuf:"bytes,1,rep,name=values,proto3" json:"values,omitempty"`
}

func (x *TenantNames) Reset() {
	*x = TenantNames{}
	if protoimpl.UnsafeEnabled {
		mi := &file_v1_tenants_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TenantNames) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TenantNames) ProtoMessage() {}

func (x *TenantNames) ProtoReflect() protoreflect.Message {
	mi := &file_v1_tenants_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TenantNames.ProtoReflect.Descriptor instead.
func (*TenantNames) Descriptor() ([]byte, []int) {
	return file_v1_tenants_proto_rawDescGZIP(), []int{1}
}

func (x *TenantNames) GetValues() []string {
	if x != nil {
		return x.Values
	}
	return nil
}

type TenantsGetReply struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Took    float32   `protobuf:"fixed32,1,opt,name=took,proto3" json:"took,omitempty"`
	Tenants []*Tenant `protobuf:"bytes,2,rep,name=tenants,proto3" json:"tenants,omitempty"`
}

func (x *TenantsGetReply) Reset() {
	*x = TenantsGetReply{}
	if protoimpl.UnsafeEnabled {
		mi := &file_v1_tenants_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TenantsGetReply) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TenantsGetReply) ProtoMessage() {}

func (x *TenantsGetReply) ProtoReflect() protoreflect.Message {
	mi := &file_v1_tenants_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TenantsGetReply.ProtoReflect.Descriptor instead.
func (*TenantsGetReply) Descriptor() ([]byte, []int) {
	return file_v1_tenants_proto_rawDescGZIP(), []int{2}
}

func (x *TenantsGetReply) GetTook() float32 {
	if x != nil {
		return x.Took
	}
	return 0
}

func (x *TenantsGetReply) GetTenants() []*Tenant {
	if x != nil {
		return x.Tenants
	}
	return nil
}

type Tenant struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name           string               `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	ActivityStatus TenantActivityStatus `protobuf:"varint,2,opt,name=activity_status,json=activityStatus,proto3,enum=weaviate.v1.TenantActivityStatus" json:"activity_status,omitempty"`
}

func (x *Tenant) Reset() {
	*x = Tenant{}
	if protoimpl.UnsafeEnabled {
		mi := &file_v1_tenants_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Tenant) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Tenant) ProtoMessage() {}

func (x *Tenant) ProtoReflect() protoreflect.Message {
	mi := &file_v1_tenants_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Tenant.ProtoReflect.Descriptor instead.
func (*Tenant) Descriptor() ([]byte, []int) {
	return file_v1_tenants_proto_rawDescGZIP(), []int{3}
}

func (x *Tenant) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *Tenant) GetActivityStatus() TenantActivityStatus {
	if x != nil {
		return x.ActivityStatus
	}
	return TenantActivityStatus_TENANT_ACTIVITY_STATUS_UNSPECIFIED
}

var File_v1_tenants_proto protoreflect.FileDescriptor

var file_v1_tenants_proto_rawDesc = []byte{
	0x0a, 0x10, 0x76, 0x31, 0x2f, 0x74, 0x65, 0x6e, 0x61, 0x6e, 0x74, 0x73, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x12, 0x0b, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x76, 0x31, 0x1a,
	0x0d, 0x76, 0x31, 0x2f, 0x62, 0x61, 0x73, 0x65, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0xd6,
	0x01, 0x0a, 0x11, 0x54, 0x65, 0x6e, 0x61, 0x6e, 0x74, 0x73, 0x47, 0x65, 0x74, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x1e, 0x0a, 0x0a, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63, 0x74, 0x69,
	0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x0a, 0x63, 0x6f, 0x6c, 0x6c, 0x65, 0x63,
	0x74, 0x69, 0x6f, 0x6e, 0x12, 0x4f, 0x0a, 0x11, 0x63, 0x6f, 0x6e, 0x73, 0x69, 0x73, 0x74, 0x65,
	0x6e, 0x63, 0x79, 0x5f, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32,
	0x1d, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x43, 0x6f,
	0x6e, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x79, 0x4c, 0x65, 0x76, 0x65, 0x6c, 0x48, 0x01,
	0x52, 0x10, 0x63, 0x6f, 0x6e, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63, 0x79, 0x4c, 0x65, 0x76,
	0x65, 0x6c, 0x88, 0x01, 0x01, 0x12, 0x30, 0x0a, 0x05, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x18, 0x03,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e,
	0x76, 0x31, 0x2e, 0x54, 0x65, 0x6e, 0x61, 0x6e, 0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x48, 0x00,
	0x52, 0x05, 0x6e, 0x61, 0x6d, 0x65, 0x73, 0x42, 0x08, 0x0a, 0x06, 0x70, 0x61, 0x72, 0x61, 0x6d,
	0x73, 0x42, 0x14, 0x0a, 0x12, 0x5f, 0x63, 0x6f, 0x6e, 0x73, 0x69, 0x73, 0x74, 0x65, 0x6e, 0x63,
	0x79, 0x5f, 0x6c, 0x65, 0x76, 0x65, 0x6c, 0x22, 0x25, 0x0a, 0x0b, 0x54, 0x65, 0x6e, 0x61, 0x6e,
	0x74, 0x4e, 0x61, 0x6d, 0x65, 0x73, 0x12, 0x16, 0x0a, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x06, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73, 0x22, 0x54,
	0x0a, 0x0f, 0x54, 0x65, 0x6e, 0x61, 0x6e, 0x74, 0x73, 0x47, 0x65, 0x74, 0x52, 0x65, 0x70, 0x6c,
	0x79, 0x12, 0x12, 0x0a, 0x04, 0x74, 0x6f, 0x6f, 0x6b, 0x18, 0x01, 0x20, 0x01, 0x28, 0x02, 0x52,
	0x04, 0x74, 0x6f, 0x6f, 0x6b, 0x12, 0x2d, 0x0a, 0x07, 0x74, 0x65, 0x6e, 0x61, 0x6e, 0x74, 0x73,
	0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74,
	0x65, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x65, 0x6e, 0x61, 0x6e, 0x74, 0x52, 0x07, 0x74, 0x65, 0x6e,
	0x61, 0x6e, 0x74, 0x73, 0x22, 0x68, 0x0a, 0x06, 0x54, 0x65, 0x6e, 0x61, 0x6e, 0x74, 0x12, 0x12,
	0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61,
	0x6d, 0x65, 0x12, 0x4a, 0x0a, 0x0f, 0x61, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74, 0x79, 0x5f, 0x73,
	0x74, 0x61, 0x74, 0x75, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x21, 0x2e, 0x77, 0x65,
	0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2e, 0x76, 0x31, 0x2e, 0x54, 0x65, 0x6e, 0x61, 0x6e, 0x74,
	0x41, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74, 0x79, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x52, 0x0e,
	0x61, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74, 0x79, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x2a, 0xc3,
	0x01, 0x0a, 0x14, 0x54, 0x65, 0x6e, 0x61, 0x6e, 0x74, 0x41, 0x63, 0x74, 0x69, 0x76, 0x69, 0x74,
	0x79, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x12, 0x26, 0x0a, 0x22, 0x54, 0x45, 0x4e, 0x41, 0x4e,
	0x54, 0x5f, 0x41, 0x43, 0x54, 0x49, 0x56, 0x49, 0x54, 0x59, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x55,
	0x53, 0x5f, 0x55, 0x4e, 0x53, 0x50, 0x45, 0x43, 0x49, 0x46, 0x49, 0x45, 0x44, 0x10, 0x00, 0x12,
	0x1e, 0x0a, 0x1a, 0x54, 0x45, 0x4e, 0x41, 0x4e, 0x54, 0x5f, 0x41, 0x43, 0x54, 0x49, 0x56, 0x49,
	0x54, 0x59, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f, 0x48, 0x4f, 0x54, 0x10, 0x01, 0x12,
	0x1f, 0x0a, 0x1b, 0x54, 0x45, 0x4e, 0x41, 0x4e, 0x54, 0x5f, 0x41, 0x43, 0x54, 0x49, 0x56, 0x49,
	0x54, 0x59, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f, 0x43, 0x4f, 0x4c, 0x44, 0x10, 0x02,
	0x12, 0x1f, 0x0a, 0x1b, 0x54, 0x45, 0x4e, 0x41, 0x4e, 0x54, 0x5f, 0x41, 0x43, 0x54, 0x49, 0x56,
	0x49, 0x54, 0x59, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f, 0x57, 0x41, 0x52, 0x4d, 0x10,
	0x03, 0x12, 0x21, 0x0a, 0x1d, 0x54, 0x45, 0x4e, 0x41, 0x4e, 0x54, 0x5f, 0x41, 0x43, 0x54, 0x49,
	0x56, 0x49, 0x54, 0x59, 0x5f, 0x53, 0x54, 0x41, 0x54, 0x55, 0x53, 0x5f, 0x46, 0x52, 0x4f, 0x5a,
	0x45, 0x4e, 0x10, 0x04, 0x42, 0x71, 0x0a, 0x23, 0x69, 0x6f, 0x2e, 0x77, 0x65, 0x61, 0x76, 0x69,
	0x61, 0x74, 0x65, 0x2e, 0x63, 0x6c, 0x69, 0x65, 0x6e, 0x74, 0x2e, 0x67, 0x72, 0x70, 0x63, 0x2e,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x2e, 0x76, 0x31, 0x42, 0x14, 0x57, 0x65, 0x61,
	0x76, 0x69, 0x61, 0x74, 0x65, 0x50, 0x72, 0x6f, 0x74, 0x6f, 0x54, 0x65, 0x6e, 0x61, 0x6e, 0x74,
	0x73, 0x5a, 0x34, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f, 0x6d, 0x2f, 0x77, 0x65,
	0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2f, 0x77, 0x65, 0x61, 0x76, 0x69, 0x61, 0x74, 0x65, 0x2f,
	0x67, 0x72, 0x70, 0x63, 0x2f, 0x67, 0x65, 0x6e, 0x65, 0x72, 0x61, 0x74, 0x65, 0x64, 0x3b, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x63, 0x6f, 0x6c, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_v1_tenants_proto_rawDescOnce sync.Once
	file_v1_tenants_proto_rawDescData = file_v1_tenants_proto_rawDesc
)

func file_v1_tenants_proto_rawDescGZIP() []byte {
	file_v1_tenants_proto_rawDescOnce.Do(func() {
		file_v1_tenants_proto_rawDescData = protoimpl.X.CompressGZIP(file_v1_tenants_proto_rawDescData)
	})
	return file_v1_tenants_proto_rawDescData
}

var file_v1_tenants_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_v1_tenants_proto_msgTypes = make([]protoimpl.MessageInfo, 4)
var file_v1_tenants_proto_goTypes = []interface{}{
	(TenantActivityStatus)(0), // 0: weaviate.v1.TenantActivityStatus
	(*TenantsGetRequest)(nil), // 1: weaviate.v1.TenantsGetRequest
	(*TenantNames)(nil),       // 2: weaviate.v1.TenantNames
	(*TenantsGetReply)(nil),   // 3: weaviate.v1.TenantsGetReply
	(*Tenant)(nil),            // 4: weaviate.v1.Tenant
	(ConsistencyLevel)(0),     // 5: weaviate.v1.ConsistencyLevel
}
var file_v1_tenants_proto_depIdxs = []int32{
	5, // 0: weaviate.v1.TenantsGetRequest.consistency_level:type_name -> weaviate.v1.ConsistencyLevel
	2, // 1: weaviate.v1.TenantsGetRequest.names:type_name -> weaviate.v1.TenantNames
	4, // 2: weaviate.v1.TenantsGetReply.tenants:type_name -> weaviate.v1.Tenant
	0, // 3: weaviate.v1.Tenant.activity_status:type_name -> weaviate.v1.TenantActivityStatus
	4, // [4:4] is the sub-list for method output_type
	4, // [4:4] is the sub-list for method input_type
	4, // [4:4] is the sub-list for extension type_name
	4, // [4:4] is the sub-list for extension extendee
	0, // [0:4] is the sub-list for field type_name
}

func init() { file_v1_tenants_proto_init() }
func file_v1_tenants_proto_init() {
	if File_v1_tenants_proto != nil {
		return
	}
	file_v1_base_proto_init()
	if !protoimpl.UnsafeEnabled {
		file_v1_tenants_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TenantsGetRequest); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_v1_tenants_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TenantNames); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_v1_tenants_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TenantsGetReply); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_v1_tenants_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Tenant); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	file_v1_tenants_proto_msgTypes[0].OneofWrappers = []interface{}{
		(*TenantsGetRequest_Names)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_v1_tenants_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   4,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_v1_tenants_proto_goTypes,
		DependencyIndexes: file_v1_tenants_proto_depIdxs,
		EnumInfos:         file_v1_tenants_proto_enumTypes,
		MessageInfos:      file_v1_tenants_proto_msgTypes,
	}.Build()
	File_v1_tenants_proto = out.File
	file_v1_tenants_proto_rawDesc = nil
	file_v1_tenants_proto_goTypes = nil
	file_v1_tenants_proto_depIdxs = nil
}
