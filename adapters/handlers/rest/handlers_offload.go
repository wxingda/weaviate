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
	"github.com/go-openapi/runtime/middleware"
	"github.com/sirupsen/logrus"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/backups"
	"github.com/weaviate/weaviate/adapters/handlers/rest/operations/offloads"
	"github.com/weaviate/weaviate/entities/models"
	eoff "github.com/weaviate/weaviate/entities/offload"
	"github.com/weaviate/weaviate/usecases/auth/authorization/errors"
	"github.com/weaviate/weaviate/usecases/monitoring"
	uoff "github.com/weaviate/weaviate/usecases/offload"
)

type offloadHandlers struct {
	scheduler *uoff.Scheduler
	// metricRequestsTotal restApiRequestsTotal
}

// compressionFromCfg transforms model backup config to a backup compression config
func compressionFromOffloadCfg(cfg *models.OffloadConfig) uoff.Compression {
	if cfg != nil {
		if cfg.CPUPercentage == 0 {
			cfg.CPUPercentage = uoff.DefaultCPUPercentage
		}

		if cfg.ChunkSize == 0 {
			cfg.ChunkSize = uoff.DefaultChunkSize
		}

		if cfg.CompressionLevel == "" {
			cfg.CompressionLevel = models.OffloadConfigCompressionLevelDefaultCompression
		}

		return uoff.Compression{
			CPUPercentage: int(cfg.CPUPercentage),
			ChunkSize:     int(cfg.ChunkSize),
			Level:         parseCompressionLevel2(cfg.CompressionLevel),
		}
	}

	return uoff.Compression{
		Level:         uoff.DefaultCompression,
		CPUPercentage: uoff.DefaultCPUPercentage,
		ChunkSize:     uoff.DefaultChunkSize,
	}
}

func compressionFromOnloadCfg(cfg *models.OnloadConfig) uoff.Compression {
	if cfg != nil {
		if cfg.CPUPercentage == 0 {
			cfg.CPUPercentage = uoff.DefaultCPUPercentage
		}

		return uoff.Compression{
			CPUPercentage: int(cfg.CPUPercentage),
			Level:         uoff.DefaultCompression,
			ChunkSize:     uoff.DefaultChunkSize,
		}
	}

	return uoff.Compression{
		Level:         uoff.DefaultCompression,
		CPUPercentage: uoff.DefaultCPUPercentage,
		ChunkSize:     uoff.DefaultChunkSize,
	}
}

func parseCompressionLevel2(l string) uoff.CompressionLevel {
	switch {
	case l == models.OffloadConfigCompressionLevelBestSpeed:
		return uoff.BestSpeed
	case l == models.OffloadConfigCompressionLevelBestCompression:
		return uoff.BestCompression
	default:
		return uoff.DefaultCompression
	}
}

func (s *offloadHandlers) offload(params offloads.OffloadParams,
	principal *models.Principal,
) middleware.Responder {
	meta, err := s.scheduler.Offload(params.HTTPRequest.Context(), principal, &uoff.OffloadRequest{
		Backend:     params.Backend,
		Class:       params.Class,
		Tenants:     params.Body.Tenants,
		Compression: compressionFromOffloadCfg(params.Body.Config),
	})
	if err != nil {
		// s.metricRequestsTotal.logError("", err)
		switch err.(type) {
		case errors.Forbidden:
			return offloads.NewOffloadForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case eoff.ErrUnprocessable:
			return offloads.NewOffloadUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return offloads.NewOffloadInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	// s.metricRequestsTotal.logOk("")
	return backups.NewBackupsCreateOK().WithPayload(meta)
}

// func (s *backupHandlers) createBackupStatus(params backups.BackupsCreateStatusParams,
// 	principal *models.Principal,
// ) middleware.Responder {
// 	status, err := s.manager.BackupStatus(params.HTTPRequest.Context(), principal, params.Backend, params.ID)
// 	if err != nil {
// 		s.metricRequestsTotal.logError("", err)
// 		switch err.(type) {
// 		case errors.Forbidden:
// 			return backups.NewBackupsCreateStatusForbidden().
// 				WithPayload(errPayloadFromSingleErr(err))
// 		case backup.ErrUnprocessable:
// 			return backups.NewBackupsCreateStatusUnprocessableEntity().
// 				WithPayload(errPayloadFromSingleErr(err))
// 		case backup.ErrNotFound:
// 			return backups.NewBackupsCreateStatusNotFound().
// 				WithPayload(errPayloadFromSingleErr(err))
// 		default:
// 			return backups.NewBackupsCreateStatusInternalServerError().
// 				WithPayload(errPayloadFromSingleErr(err))
// 		}
// 	}

// 	strStatus := string(status.Status)
// 	payload := models.BackupCreateStatusResponse{
// 		Status:  &strStatus,
// 		ID:      params.ID,
// 		Path:    status.Path,
// 		Backend: params.Backend,
// 		Error:   status.Err,
// 	}
// 	s.metricRequestsTotal.logOk("")
// 	return backups.NewBackupsCreateStatusOK().WithPayload(&payload)
// }

func (s *offloadHandlers) onload(params offloads.OnloadParams,
	principal *models.Principal,
) middleware.Responder {
	meta, err := s.scheduler.Onload(params.HTTPRequest.Context(), principal, &uoff.OffloadRequest{
		Backend:     params.Backend,
		Class:       params.Class,
		Tenants:     params.Body.Tenants,
		NodeMapping: params.Body.NodeMapping,
		Compression: compressionFromOnloadCfg(params.Body.Config),
	})
	if err != nil {
		// s.metricRequestsTotal.logError("", err)
		switch err.(type) {
		case errors.Forbidden:
			return offloads.NewOnloadForbidden().
				WithPayload(errPayloadFromSingleErr(err))
		case eoff.ErrNotFound:
			return offloads.NewOnloadNotFound().
				WithPayload(errPayloadFromSingleErr(err))
		case eoff.ErrUnprocessable:
			return offloads.NewOnloadUnprocessableEntity().
				WithPayload(errPayloadFromSingleErr(err))
		default:
			return offloads.NewOnloadInternalServerError().
				WithPayload(errPayloadFromSingleErr(err))
		}
	}

	// s.metricRequestsTotal.logOk("")
	return backups.NewBackupsRestoreOK().WithPayload(meta)
}

// func (s *backupHandlers) restoreBackupStatus(params backups.BackupsRestoreStatusParams,
// 	principal *models.Principal,
// ) middleware.Responder {
// 	status, err := s.manager.RestorationStatus(
// 		params.HTTPRequest.Context(), principal, params.Backend, params.ID)
// 	if err != nil {
// 		s.metricRequestsTotal.logError("", err)
// 		switch err.(type) {
// 		case errors.Forbidden:
// 			return backups.NewBackupsRestoreForbidden().
// 				WithPayload(errPayloadFromSingleErr(err))
// 		case backup.ErrNotFound:
// 			return backups.NewBackupsRestoreNotFound().
// 				WithPayload(errPayloadFromSingleErr(err))
// 		case backup.ErrUnprocessable:
// 			return backups.NewBackupsRestoreUnprocessableEntity().
// 				WithPayload(errPayloadFromSingleErr(err))
// 		default:
// 			return backups.NewBackupsRestoreInternalServerError().
// 				WithPayload(errPayloadFromSingleErr(err))
// 		}
// 	}
// 	strStatus := string(status.Status)
// 	payload := models.BackupRestoreStatusResponse{
// 		Status:  &strStatus,
// 		ID:      params.ID,
// 		Path:    status.Path,
// 		Backend: params.Backend,
// 		Error:   status.Err,
// 	}
// 	s.metricRequestsTotal.logOk("")
// 	return backups.NewBackupsRestoreStatusOK().WithPayload(&payload)
// }

func setupOffloadHandlers(api *operations.WeaviateAPI,
	scheduler *uoff.Scheduler, metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger,
) {
	h := &offloadHandlers{scheduler}
	api.OffloadsOffloadHandler = offloads.OffloadHandlerFunc(h.offload)
	api.OffloadsOnloadHandler = offloads.OnloadHandlerFunc(h.onload)
}

// type backupRequestsTotal struct {
// 	*restApiRequestsTotalImpl
// }

// func newBackupRequestsTotal(metrics *monitoring.PrometheusMetrics, logger logrus.FieldLogger) restApiRequestsTotal {
// 	return &backupRequestsTotal{
// 		restApiRequestsTotalImpl: &restApiRequestsTotalImpl{newRequestsTotalMetric(metrics, "rest"), "rest", "backup", logger},
// 	}
// }

// func (e *backupRequestsTotal) logError(className string, err error) {
// 	switch err.(type) {
// 	case errors.Forbidden:
// 		e.logUserError(className)
// 	case backup.ErrUnprocessable, backup.ErrNotFound:
// 		e.logUserError(className)
// 	default:
// 		e.logServerError(className, err)
// 	}
// }
