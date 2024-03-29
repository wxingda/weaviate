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

package rest

import (
	"context"
	"fmt"

	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/schema"
	"github.com/weaviate/weaviate/entities/models"
	eoff "github.com/weaviate/weaviate/entities/offload"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	uco "github.com/weaviate/weaviate/usecases/objects"
	"github.com/weaviate/weaviate/usecases/offload"
	schemaUC "github.com/weaviate/weaviate/usecases/schema"
)

type schemaHandlers struct {
	manager             *schemaUC.Manager
	offloadScheduler    *offload.Scheduler
	metricRequestsTotal restApiRequestsTotal
}

func (s *schemaHandlers) addClass(params schema.SchemaObjectsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	_, _, err := s.manager.AddClass(params.HTTPRequest.Context(), principal, params.ObjectClass)
	if err != nil {
		s.metricRequestsTotal.logError(params.ObjectClass.Class, err)
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk(params.ObjectClass.Class)
	return schema.NewSchemaObjectsCreateOK().WithPayload(params.ObjectClass)
}

func (s *schemaHandlers) updateClass(params schema.SchemaObjectsUpdateParams,
	principal *models.Principal,
) middleware.Responder {
	err := s.manager.UpdateClass(params.HTTPRequest.Context(), principal, params.ClassName,
		params.ObjectClass)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		if err == schemaUC.ErrNotFound {
			return schema.NewSchemaObjectsUpdateNotFound()
		}

		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsUpdateOK().WithPayload(params.ObjectClass)
}

func (s *schemaHandlers) getClass(params schema.SchemaObjectsGetParams,
	principal *models.Principal,
) middleware.Responder {
	class, _, err := s.manager.GetConsistentClass(params.HTTPRequest.Context(), principal, params.ClassName, *params.Consistency)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsGetInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	if class == nil {
		s.metricRequestsTotal.logUserError(params.ClassName)
		return schema.NewSchemaObjectsGetNotFound()
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsGetOK().WithPayload(class)
}

func (s *schemaHandlers) deleteClass(params schema.SchemaObjectsDeleteParams, principal *models.Principal) middleware.Responder {
	err := s.manager.DeleteClass(params.HTTPRequest.Context(), principal, params.ClassName)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsDeleteBadRequest().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsDeleteOK()
}

func (s *schemaHandlers) addClassProperty(params schema.SchemaObjectsPropertiesAddParams,
	principal *models.Principal,
) middleware.Responder {
	_, _, err := s.manager.AddClassProperty(params.HTTPRequest.Context(), principal, s.manager.ReadOnlyClass(params.ClassName), false, params.Body)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsPropertiesAddForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsPropertiesAddUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewSchemaObjectsPropertiesAddOK().WithPayload(params.Body)
}

func (s *schemaHandlers) getSchema(params schema.SchemaDumpParams, principal *models.Principal) middleware.Responder {
	dbSchema, err := s.manager.GetConsistentSchema(principal, *params.Consistency)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaDumpForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaDumpForbidden().WithPayload(errPayloadFromSingleErr(err))
		}
	}

	payload := dbSchema.Objects

	s.metricRequestsTotal.logOk("")
	return schema.NewSchemaDumpOK().WithPayload(payload)
}

func (s *schemaHandlers) getShardsStatus(params schema.SchemaObjectsShardsGetParams,
	principal *models.Principal,
) middleware.Responder {
	// TODO-RAFT START
	// Fix changed interface GetShardsStatus -> ShardsStatus
	// Previous definition:
	// status, err := s.manager.GetShardsStatus(params.HTTPRequest.Context(), principal, params.ClassName, tenant)
	// var tenant string
	// if params.Tenant == nil {
	// 	tenant = ""
	// } else {
	// 	tenant = *params.Tenant
	// }

	status, err := s.manager.ShardsStatus(params.HTTPRequest.Context(), principal, params.ClassName, tenant)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsShardsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsShardsGetNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	payload := status

	s.metricRequestsTotal.logOk("")
	return schema.NewSchemaObjectsShardsGetOK().WithPayload(payload)
}

func (s *schemaHandlers) updateShardStatus(params schema.SchemaObjectsShardsUpdateParams,
	principal *models.Principal,
) middleware.Responder {
	_, err := s.manager.UpdateShardStatus(
		params.HTTPRequest.Context(), principal, params.ClassName, params.ShardName, params.Body.Status)
	if err != nil {
		s.metricRequestsTotal.logError("", err)
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewSchemaObjectsShardsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewSchemaObjectsShardsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	payload := params.Body

	s.metricRequestsTotal.logOk("")
	return schema.NewSchemaObjectsShardsUpdateOK().WithPayload(payload)
}

func (s *schemaHandlers) createTenants(params schema.TenantsCreateParams,
	principal *models.Principal,
) middleware.Responder {
	err := s.manager.AddTenants(
		params.HTTPRequest.Context(), principal, params.ClassName, params.Body)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewTenantsCreateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewTenantsCreateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsCreateOK() //.WithPayload(created)
}

func (s *schemaHandlers) updateTenants(params schema.TenantsUpdateParams,
	principal *models.Principal,
) middleware.Responder {
	ctx := params.HTTPRequest.Context()
	// TODO AL improve error handling (each tenant handler separately?)
	// currently it is unknown what is going on with remaining tentants if single one fails
	transitions, err := s.manager.UpdateTenants(
		ctx, principal, params.ClassName, params.Body)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewTenantsUpdateForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewTenantsUpdateUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	// further processing needed to offload/load tenant (async)
	offloads := map[string]*schemaUC.TenantStatusTransition{}
	loads := map[string]*schemaUC.TenantStatusTransition{}
	for name, transition := range transitions {
		if transition.To == models.TenantActivityStatusFROZEN &&
			transition.From == models.TenantActivityStatusHOT {
			offloads[name] = transition
		}
		if transition.To == models.TenantActivityStatusHOT &&
			transition.From == models.TenantActivityStatusFROZEN {
			loads[name] = transition
		}
	}

	if len(offloads) > 0 {
		compression := offload.Compression{
			Level:         offload.DefaultCompression,
			CPUPercentage: offload.DefaultCPUPercentage,
			ChunkSize:     offload.DefaultChunkSize,
		}

		for _, o := range offloads {
			req := &offload.OffloadRequest{
				Class:       params.ClassName,
				Tenant:      o.Name,
				Backend:     "filesystem", // TODO AL make configurable, move to offloader?
				Compression: compression,  // TODO AL remove? move to offloader?
			}

			// TODO AL change callback to run in worker?
			err := s.offloadScheduler.OffloadSimple(ctx, req,
				func(desc *eoff.OffloadDistributedDescriptor) {
					fmt.Printf("  ==> offload status %q\n", desc.Status)
					if desc.Status == eoff.Success {
						if err := s.manager.UpdateTenantsValidated(context.Background(), params.ClassName,
							[]*schemaUC.TenantStatusTransition{{
								Name: o.Name,
								From: models.TenantActivityStatusFROZENOFFLOAD,
								To:   models.TenantActivityStatusFROZEN,
							}}); err != nil {
							// TODO AL log error
							fmt.Printf("  ==> offload commit error %s\n", err)
						}
					} else {
						// TODO AL log error/issue
						if err := s.manager.UpdateTenantsValidated(context.Background(), params.ClassName,
							[]*schemaUC.TenantStatusTransition{{
								Name: o.Name,
								From: models.TenantActivityStatusFROZENOFFLOAD,
								To:   models.TenantActivityStatusHOT,
							}}); err != nil {
							// TODO AL log error
							fmt.Printf("  ==> offload revert error %s\n", err)
						}
					}
				},
			)
			if err != nil {
				return schema.NewTenantsUpdateUnprocessableEntity().
					WithPayload(errPayloadFromSingleErr(err))
			}
		}
	}

	for name, transition := range transitions {
		fmt.Printf("transition %q %v\n\n", name, transition)
	}
	for name, transition := range offloads {
		fmt.Printf("offloads %q %v\n\n", name, transition)
	}
	for name, transition := range loads {
		fmt.Printf("loads %q %v\n\n", name, transition)
	}

	// TODO AL adjust payload to intermediate statuses when offload/load
	payload := params.Body

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsUpdateOK().WithPayload(payload)
}

func (s *schemaHandlers) deleteTenants(params schema.TenantsDeleteParams,
	principal *models.Principal,
) middleware.Responder {
	err := s.manager.DeleteTenants(
		params.HTTPRequest.Context(), principal, params.ClassName, params.Tenants)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewTenantsDeleteForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewTenantsDeleteUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsDeleteOK()
}

func (s *schemaHandlers) getTenants(params schema.TenantsGetParams,
	principal *models.Principal,
) middleware.Responder {
	tenants, err := s.manager.GetConsistentTenants(params.HTTPRequest.Context(), principal, params.ClassName, *params.Consistency, nil)
	if err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewTenantsGetForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewTenantsGetUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	s.metricRequestsTotal.logOk(params.ClassName)
	return schema.NewTenantsGetOK().WithPayload(tenants)
}

func (s *schemaHandlers) tenantExists(params schema.TenantExistsParams, principal *models.Principal) middleware.Responder {
	if err := s.manager.ConsistentTenantExists(params.HTTPRequest.Context(), principal, params.ClassName, *params.Consistency, params.TenantName); err != nil {
		s.metricRequestsTotal.logError(params.ClassName, err)
		if err == schemaUC.ErrNotFound {
			return schema.NewTenantExistsNotFound()
		}
		switch err.(type) {
		case errors.Forbidden:
			return schema.NewTenantExistsForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return schema.NewTenantExistsUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	return schema.NewTenantExistsOK()
}

func setupSchemaHandlers(api *operations.WeaviateAPI, manager *schemaUC.Manager, offloadScheduler *offload.Scheduler,
	metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger,
) {
	h := &schemaHandlers{
		manager:             manager,
		offloadScheduler:    offloadScheduler,
		metricRequestsTotal: newSchemaRequestsTotal(metrics, logger),
	}

	api.SchemaSchemaObjectsCreateHandler = schema.
		SchemaObjectsCreateHandlerFunc(h.addClass)
	api.SchemaSchemaObjectsDeleteHandler = schema.
		SchemaObjectsDeleteHandlerFunc(h.deleteClass)
	api.SchemaSchemaObjectsPropertiesAddHandler = schema.
		SchemaObjectsPropertiesAddHandlerFunc(h.addClassProperty)

	api.SchemaSchemaObjectsUpdateHandler = schema.
		SchemaObjectsUpdateHandlerFunc(h.updateClass)

	api.SchemaSchemaObjectsGetHandler = schema.
		SchemaObjectsGetHandlerFunc(h.getClass)
	api.SchemaSchemaDumpHandler = schema.
		SchemaDumpHandlerFunc(h.getSchema)

	api.SchemaSchemaObjectsShardsGetHandler = schema.
		SchemaObjectsShardsGetHandlerFunc(h.getShardsStatus)
	api.SchemaSchemaObjectsShardsUpdateHandler = schema.
		SchemaObjectsShardsUpdateHandlerFunc(h.updateShardStatus)

	api.SchemaTenantsCreateHandler = schema.TenantsCreateHandlerFunc(h.createTenants)
	api.SchemaTenantsUpdateHandler = schema.TenantsUpdateHandlerFunc(h.updateTenants)
	api.SchemaTenantsDeleteHandler = schema.TenantsDeleteHandlerFunc(h.deleteTenants)
	api.SchemaTenantsGetHandler = schema.TenantsGetHandlerFunc(h.getTenants)
	api.SchemaTenantExistsHandler = schema.TenantExistsHandlerFunc(h.tenantExists)
}

type schemaRequestsTotal struct {
	*restApiRequestsTotalImpl
}

func newSchemaRequestsTotal(metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) restApiRequestsTotal {
	return &schemaRequestsTotal{
		restApiRequestsTotalImpl: &restApiRequestsTotalImpl{newRequestsTotalMetric(metrics, "rest"), "rest", "schema", logger},
	}
}

func (e *schemaRequestsTotal) logError(className string, err error) {
	switch err.(type) {
	case uco.ErrMultiTenancy:
		e.logUserError(className)
	case errors.Forbidden:
		e.logUserError(className)
	default:
		e.logUserError(className)
	}
}
