//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2024 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

package schema

/// TODO-RAFT START
/// Fix unit tests
/// TODO-RAFT END

// import (
// 	"context"
// 	"testing"

// 	"github.com/stretchr/testify/assert"
// 	"github.com/weaviate/weaviate/entities/models"
// 	"github.com/weaviate/weaviate/entities/schema"
// )

// func TestAddTenants(t *testing.T) {
// 	var (
// 		ctx        = context.Background()
// 		mt         = &models.MultiTenancyConfig{Enabled: true}
// 		tenants    = []*models.Tenant{{Name: "USER1"}, {Name: "USER2"}}
// 		cls        = "C1"
// 		properties = []*models.Property{
// 			{
// 				Name:     "uUID",
// 				DataType: schema.DataTypeText.PropString(),
// 			},
// 		}
// 		repConfig = &models.ReplicationConfig{Factor: 1}
// 	)

// 	type test struct {
// 		name    string
// 		Class   string
// 		tenants []*models.Tenant
// 		initial *models.Class
// 		errMsgs []string
// 	}
// 	tests := []test{
// 		{
// 			name:    "UnknownClass",
// 			Class:   "UnknownClass",
// 			tenants: tenants,
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: mt,
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{ErrNotFound.Error()},
// 		},
// 		{
// 			name:    "MTIsNil",
// 			Class:   cls,
// 			tenants: tenants,
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: nil,
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{"not enabled"},
// 		},
// 		{
// 			name:    "MTDisabled",
// 			Class:   cls,
// 			tenants: tenants,
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{"not enabled"},
// 		},
// 		{
// 			name:    "EmptyTenantValue",
// 			Class:   cls,
// 			tenants: []*models.Tenant{{Name: "Aaaa"}, {Name: ""}, {Name: "Bbbb"}},
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{"tenant"},
// 		},
// 		{
// 			name:  "InvalidActivityStatus",
// 			Class: cls,
// 			tenants: []*models.Tenant{
// 				{Name: "Aaaa", ActivityStatus: "DOES_NOT_EXIST_1"},
// 				{Name: "Bbbb", ActivityStatus: "DOES_NOT_EXIST_2"},
// 			},
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{
// 				"invalid activity status",
// 				"DOES_NOT_EXIST_1",
// 				"DOES_NOT_EXIST_2",
// 			},
// 		},
// 		{
// 			name:  "UnsupportedActivityStatus",
// 			Class: cls,
// 			tenants: []*models.Tenant{
// 				{Name: "Aaaa", ActivityStatus: models.TenantActivityStatusWARM},
// 				{Name: "Bbbb", ActivityStatus: models.TenantActivityStatusFROZEN},
// 			},
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{
// 				"not yet supported activity status",
// 				models.TenantActivityStatusWARM,
// 				models.TenantActivityStatusFROZEN,
// 			},
// 		},
// 		{
// 			name:  "Success",
// 			Class: cls,
// 			tenants: []*models.Tenant{
// 				{Name: "Aaaa"},
// 				{Name: "Bbbb", ActivityStatus: models.TenantActivityStatusHOT},
// 				{Name: "Cccc", ActivityStatus: models.TenantActivityStatusCOLD},
// 			},
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{},
// 		},
// 		// TODO test with replication factor >= 2
// 	}

// 	// AddTenants
// 	for _, test := range tests {
// 		sm := newSchemaManager()
// 		err := sm.AddClass(ctx, nil, test.initial)
// 		if err == nil {
// 			_, err = sm.AddTenants(ctx, nil, test.Class, test.tenants)
// 		}
// 		if len(test.errMsgs) == 0 {
// 			assert.Nil(t, err)
// 			ss := sm.schemaCache.ShardingState[test.Class]
// 			assert.NotNil(t, ss, test.name)
// 			assert.Equal(t, len(ss.Physical), len(test.tenants), test.name)
// 		} else {
// 			for _, msg := range test.errMsgs {
// 				assert.ErrorContains(t, err, msg, test.name)
// 			}
// 		}
// 	}
// }

// func TestUpdateTenants(t *testing.T) {
// 	var (
// 		ctx     = context.Background()
// 		mt      = &models.MultiTenancyConfig{Enabled: true}
// 		tenants = []*models.Tenant{
// 			{Name: "USER1", ActivityStatus: models.TenantActivityStatusHOT},
// 			{Name: "USER2", ActivityStatus: models.TenantActivityStatusHOT},
// 		}
// 		cls        = "C1"
// 		properties = []*models.Property{
// 			{
// 				Name:     "uUID",
// 				DataType: schema.DataTypeText.PropString(),
// 			},
// 		}
// 		repConfig = &models.ReplicationConfig{Factor: 1}
// 	)

// 	type test struct {
// 		name          string
// 		Class         string
// 		updateTenants []*models.Tenant
// 		initial       *models.Class
// 		errMsgs       []string
// 		skipAdd       bool
// 	}
// 	tests := []test{
// 		{
// 			name:          "UnknownClass",
// 			Class:         "UnknownClass",
// 			updateTenants: tenants,
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: mt,
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{ErrNotFound.Error()},
// 		},
// 		{
// 			name:          "MTIsNil",
// 			Class:         cls,
// 			updateTenants: tenants,
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: nil,
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{"not enabled"},
// 			skipAdd: true,
// 		},
// 		{
// 			name:          "MTDisabled",
// 			Class:         cls,
// 			updateTenants: tenants,
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{"not enabled"},
// 			skipAdd: true,
// 		},
// 		{
// 			name:          "EmptyTenantValue",
// 			Class:         cls,
// 			updateTenants: []*models.Tenant{{Name: ""}},
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{"tenant"},
// 		},
// 		{
// 			name:  "InvalidActivityStatus",
// 			Class: cls,
// 			updateTenants: []*models.Tenant{
// 				{Name: tenants[0].Name, ActivityStatus: "DOES_NOT_EXIST_1"},
// 				{Name: tenants[1].Name, ActivityStatus: "DOES_NOT_EXIST_2"},
// 			},
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{
// 				"invalid activity status",
// 				"DOES_NOT_EXIST_1",
// 				"DOES_NOT_EXIST_2",
// 			},
// 		},
// 		{
// 			name:  "UnsupportedActivityStatus",
// 			Class: cls,
// 			updateTenants: []*models.Tenant{
// 				{Name: tenants[0].Name, ActivityStatus: models.TenantActivityStatusWARM},
// 				{Name: tenants[1].Name, ActivityStatus: models.TenantActivityStatusFROZEN},
// 			},
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{
// 				"not yet supported activity status",
// 				models.TenantActivityStatusWARM,
// 				models.TenantActivityStatusFROZEN,
// 			},
// 		},
// 		{
// 			name:  "EmptyActivityStatus",
// 			Class: cls,
// 			updateTenants: []*models.Tenant{
// 				{Name: tenants[0].Name},
// 				{Name: tenants[1].Name, ActivityStatus: ""},
// 			},
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{"invalid activity status"},
// 		},
// 		{
// 			name:  "Success",
// 			Class: cls,
// 			updateTenants: []*models.Tenant{
// 				{Name: tenants[0].Name, ActivityStatus: models.TenantActivityStatusCOLD},
// 				{Name: tenants[1].Name, ActivityStatus: models.TenantActivityStatusCOLD},
// 			},
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsgs: []string{},
// 		},
// 	}

// 	for _, test := range tests {
// 		sm := newSchemaManager()
// 		if err := sm.AddClass(ctx, nil, test.initial); err != nil {
// 			t.Fatalf("%s: add class: %v", test.name, err)
// 		}
// 		if !test.skipAdd {
// 			if _, err := sm.AddTenants(ctx, nil, cls, tenants); err != nil {
// 				t.Fatalf("%s: add tenants: %v", test.name, err)
// 			}
// 		}

// 		err := sm.UpdateTenants(ctx, nil, test.Class, test.updateTenants)
// 		if len(test.errMsgs) == 0 {
// 			if err != nil {
// 				t.Fatalf("%s: update tenants: %v", test.name, err)
// 			}
// 			ss := sm.schemaCache.ShardingState[test.Class]
// 			if ss == nil {
// 				t.Fatalf("%s: sharding state equal nil", test.name)
// 			}

// 			assert.Len(t, ss.Physical, len(tenants))
// 			for _, tenant := range test.updateTenants {
// 				assert.Equal(t, tenant.ActivityStatus, ss.Physical[tenant.Name].Status, test.name)
// 			}
// 		} else {
// 			for _, msg := range test.errMsgs {
// 				assert.ErrorContains(t, err, msg, test.name)
// 			}
// 		}
// 	}
// }

// func TestDeleteTenants(t *testing.T) {
// 	var (
// 		ctx     = context.Background()
// 		tenants = []*models.Tenant{
// 			{Name: "USER1"},
// 			{Name: "USER2"},
// 			{Name: "USER3"},
// 			{Name: "USER4"},
// 		}
// 		cls        = "C1"
// 		properties = []*models.Property{
// 			{
// 				Name:     "uUID",
// 				DataType: schema.DataTypeText.PropString(),
// 			},
// 		}
// 		repConfig = &models.ReplicationConfig{Factor: 1}
// 	)

// 	type test struct {
// 		name       string
// 		Class      string
// 		tenants    []*models.Tenant
// 		initial    *models.Class
// 		errMsg     string
// 		addTenants bool
// 	}
// 	tests := []test{
// 		{
// 			name:    "UnknownClass",
// 			Class:   "UnknownClass",
// 			tenants: tenants,
// 			initial: &models.Class{
// 				Class: cls, MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
// 				Properties:        properties,
// 				ReplicationConfig: repConfig,
// 			},
// 			errMsg: ErrNotFound.Error(),
// 		},
// 		{
// 			name:    "MTIsNil",
// 			Class:   "C1",
// 			tenants: tenants,
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: nil,
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsg: "not enabled",
// 		},
// 		{
// 			name:    "MTDisabled",
// 			Class:   "C1",
// 			tenants: tenants,
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsg: "not enabled",
// 		},
// 		{
// 			name:    "EmptyTenantValue",
// 			Class:   "C1",
// 			tenants: []*models.Tenant{{Name: "Aaaa"}, {Name: ""}, {Name: "Bbbb"}},
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsg: "empty tenant name at index 1",
// 		},
// 		{
// 			name:    "Success",
// 			Class:   "C1",
// 			tenants: tenants[:2],
// 			initial: &models.Class{
// 				Class:              cls,
// 				MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
// 				Properties:         properties,
// 				ReplicationConfig:  repConfig,
// 			},
// 			errMsg:     "",
// 			addTenants: true,
// 		},
// 	}

// 	for _, test := range tests {
// 		sm := newSchemaManager()
// 		err := sm.AddClass(ctx, nil, test.initial)
// 		if err != nil {
// 			t.Fatalf("%s: add class: %v", test.name, err)
// 		}
// 		if test.addTenants {
// 			_, err = sm.AddTenants(ctx, nil, test.Class, tenants)
// 			if err != nil {
// 				t.Fatalf("%s: add tenants: %v", test.name, err)
// 			}
// 		}
// 		var tenantNames []string
// 		for i := range test.tenants {
// 			tenantNames = append(tenantNames, test.tenants[i].Name)
// 		}

// 		err = sm.DeleteTenants(ctx, nil, test.Class, tenantNames)
// 		if test.errMsg == "" {
// 			if err != nil {
// 				t.Fatalf("%s: remove tenants: %v", test.name, err)
// 			}
// 			ss := sm.schemaCache.ShardingState[test.Class]
// 			if ss == nil {
// 				t.Fatalf("%s: sharding state equal nil", test.name)
// 			}
// 			assert.Equal(t, len(test.tenants)+len(ss.Physical), len(tenants))
// 		} else {
// 			assert.ErrorContains(t, err, test.errMsg, test.name)
// 		}

			_, err := handler.UpdateTenants(ctx, nil, test.class, test.updateTenants)
			if len(test.errMsgs) == 0 {
				require.NoError(t, err)
			} else {
				for i := range test.errMsgs {
					assert.ErrorContains(t, err, test.errMsgs[i])
				}
			}

			fakeMetaHandler.AssertExpectations(t)
		})
	}
}

func TestDeleteTenants(t *testing.T) {
	var (
		ctx     = context.Background()
		tenants = []*models.Tenant{
			{Name: "USER1"},
			{Name: "USER2"},
			{Name: "USER3"},
			{Name: "USER4"},
		}
		properties = []*models.Property{
			{
				Name:     "uuid",
				DataType: schema.DataTypeText.PropString(),
			},
		}
		repConfig = &models.ReplicationConfig{Factor: 1}
	)

	mtNilClass := &models.Class{
		Class:              "MTnil",
		MultiTenancyConfig: nil,
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}
	mtDisabledClass := &models.Class{
		Class:              "MTdisabled",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: false},
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}
	mtEnabledClass := &models.Class{
		Class:              "MTenabled",
		MultiTenancyConfig: &models.MultiTenancyConfig{Enabled: true},
		Properties:         properties,
		ReplicationConfig:  repConfig,
		Vectorizer:         "none",
	}

	type test struct {
		name            string
		class           string
		tenants         []*models.Tenant
		errMsgs         []string
		expectedTenants []*models.Tenant
		mockCalls       func(fakeMetaHandler *fakeMetaHandler)
	}

	tests := []test{
		{
			name:            "MTIsNil",
			class:           mtNilClass.Class,
			tenants:         tenants,
			errMsgs:         []string{"not enabled"},
			expectedTenants: tenants,
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("ClassInfo", mock.Anything).Return(
					store.ClassInfo{Exists: true, MultiTenancy: models.MultiTenancyConfig{}})
			},
		},
		{
			name:            "MTDisabled",
			class:           mtDisabledClass.Class,
			tenants:         tenants,
			errMsgs:         []string{"not enabled"},
			expectedTenants: tenants,
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("ClassInfo", mock.Anything).Return(
					store.ClassInfo{Exists: true, MultiTenancy: models.MultiTenancyConfig{}})
			},
		},
		{
			name:            "UnknownClass",
			class:           "UnknownClass",
			tenants:         tenants,
			errMsgs:         []string{ErrNotFound.Error()},
			expectedTenants: tenants,
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("ClassInfo", mock.Anything).Return(
					store.ClassInfo{Exists: false, MultiTenancy: models.MultiTenancyConfig{}})
			},
		},
		{
			name:  "EmptyTenantValue",
			class: mtEnabledClass.Class,
			tenants: []*models.Tenant{
				{Name: "Aaaa"},
				{Name: ""},
				{Name: "Bbbb"},
			},
			errMsgs:         []string{"empty tenant name at index 1"},
			expectedTenants: tenants,
			mockCalls:       func(fakeMetaHandler *fakeMetaHandler) {},
		},
		{
			name:            "Success",
			class:           mtEnabledClass.Class,
			tenants:         tenants[:2],
			errMsgs:         []string{},
			expectedTenants: tenants[2:],
			mockCalls: func(fakeMetaHandler *fakeMetaHandler) {
				fakeMetaHandler.On("ClassInfo", mock.Anything).Return(
					store.ClassInfo{Exists: true, MultiTenancy: models.MultiTenancyConfig{Enabled: true}})
				fakeMetaHandler.On("DeleteTenants", mock.Anything, mock.Anything).Return(nil)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Isolate schema for each tests
			handler, fakeMetaHandler := newTestHandler(t, &fakeDB{})
			test.mockCalls(fakeMetaHandler)

			tenantNames := make([]string, len(test.tenants))
			for i := range test.tenants {
				tenantNames[i] = test.tenants[i].Name
			}

			err := handler.DeleteTenants(ctx, nil, test.class, tenantNames)
			if len(test.errMsgs) == 0 {
				require.NoError(t, err)
			} else {
				for i := range test.errMsgs {
					assert.ErrorContains(t, err, test.errMsgs[i])
				}
			}
		})
	}
}
