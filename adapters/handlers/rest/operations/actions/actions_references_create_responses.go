//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2020 SeMI Technologies B.V. All rights reserved.
//
//  CONTACT: hello@semi.technology
//

// Code generated by go-swagger; DO NOT EDIT.

package actions

// This file was generated by the swagger tool.
// Editing this file might prove futile when you re-run the swagger generate command

import (
	"net/http"

	"github.com/go-openapi/runtime"

	"github.com/semi-technologies/weaviate/entities/models"
)

// ActionsReferencesCreateOKCode is the HTTP code returned for type ActionsReferencesCreateOK
const ActionsReferencesCreateOKCode int = 200

/*ActionsReferencesCreateOK Successfully added the reference.

swagger:response actionsReferencesCreateOK
*/
type ActionsReferencesCreateOK struct {
}

// NewActionsReferencesCreateOK creates ActionsReferencesCreateOK with default headers values
func NewActionsReferencesCreateOK() *ActionsReferencesCreateOK {

	return &ActionsReferencesCreateOK{}
}

// WriteResponse to the client
func (o *ActionsReferencesCreateOK) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(200)
}

// ActionsReferencesCreateUnauthorizedCode is the HTTP code returned for type ActionsReferencesCreateUnauthorized
const ActionsReferencesCreateUnauthorizedCode int = 401

/*ActionsReferencesCreateUnauthorized Unauthorized or invalid credentials.

swagger:response actionsReferencesCreateUnauthorized
*/
type ActionsReferencesCreateUnauthorized struct {
}

// NewActionsReferencesCreateUnauthorized creates ActionsReferencesCreateUnauthorized with default headers values
func NewActionsReferencesCreateUnauthorized() *ActionsReferencesCreateUnauthorized {

	return &ActionsReferencesCreateUnauthorized{}
}

// WriteResponse to the client
func (o *ActionsReferencesCreateUnauthorized) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.Header().Del(runtime.HeaderContentType) //Remove Content-Type on empty responses

	rw.WriteHeader(401)
}

// ActionsReferencesCreateForbiddenCode is the HTTP code returned for type ActionsReferencesCreateForbidden
const ActionsReferencesCreateForbiddenCode int = 403

/*ActionsReferencesCreateForbidden Forbidden

swagger:response actionsReferencesCreateForbidden
*/
type ActionsReferencesCreateForbidden struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewActionsReferencesCreateForbidden creates ActionsReferencesCreateForbidden with default headers values
func NewActionsReferencesCreateForbidden() *ActionsReferencesCreateForbidden {

	return &ActionsReferencesCreateForbidden{}
}

// WithPayload adds the payload to the actions references create forbidden response
func (o *ActionsReferencesCreateForbidden) WithPayload(payload *models.ErrorResponse) *ActionsReferencesCreateForbidden {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the actions references create forbidden response
func (o *ActionsReferencesCreateForbidden) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ActionsReferencesCreateForbidden) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(403)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ActionsReferencesCreateUnprocessableEntityCode is the HTTP code returned for type ActionsReferencesCreateUnprocessableEntity
const ActionsReferencesCreateUnprocessableEntityCode int = 422

/*ActionsReferencesCreateUnprocessableEntity Request body is well-formed (i.e., syntactically correct), but semantically erroneous. Are you sure the property exists or that it is a class?

swagger:response actionsReferencesCreateUnprocessableEntity
*/
type ActionsReferencesCreateUnprocessableEntity struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewActionsReferencesCreateUnprocessableEntity creates ActionsReferencesCreateUnprocessableEntity with default headers values
func NewActionsReferencesCreateUnprocessableEntity() *ActionsReferencesCreateUnprocessableEntity {

	return &ActionsReferencesCreateUnprocessableEntity{}
}

// WithPayload adds the payload to the actions references create unprocessable entity response
func (o *ActionsReferencesCreateUnprocessableEntity) WithPayload(payload *models.ErrorResponse) *ActionsReferencesCreateUnprocessableEntity {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the actions references create unprocessable entity response
func (o *ActionsReferencesCreateUnprocessableEntity) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ActionsReferencesCreateUnprocessableEntity) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(422)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}

// ActionsReferencesCreateInternalServerErrorCode is the HTTP code returned for type ActionsReferencesCreateInternalServerError
const ActionsReferencesCreateInternalServerErrorCode int = 500

/*ActionsReferencesCreateInternalServerError An error has occurred while trying to fulfill the request. Most likely the ErrorResponse will contain more information about the error.

swagger:response actionsReferencesCreateInternalServerError
*/
type ActionsReferencesCreateInternalServerError struct {

	/*
	  In: Body
	*/
	Payload *models.ErrorResponse `json:"body,omitempty"`
}

// NewActionsReferencesCreateInternalServerError creates ActionsReferencesCreateInternalServerError with default headers values
func NewActionsReferencesCreateInternalServerError() *ActionsReferencesCreateInternalServerError {

	return &ActionsReferencesCreateInternalServerError{}
}

// WithPayload adds the payload to the actions references create internal server error response
func (o *ActionsReferencesCreateInternalServerError) WithPayload(payload *models.ErrorResponse) *ActionsReferencesCreateInternalServerError {
	o.Payload = payload
	return o
}

// SetPayload sets the payload to the actions references create internal server error response
func (o *ActionsReferencesCreateInternalServerError) SetPayload(payload *models.ErrorResponse) {
	o.Payload = payload
}

// WriteResponse to the client
func (o *ActionsReferencesCreateInternalServerError) WriteResponse(rw http.ResponseWriter, producer runtime.Producer) {

	rw.WriteHeader(500)
	if o.Payload != nil {
		payload := o.Payload
		if err := producer.Produce(rw, payload); err != nil {
			panic(err) // let the recovery middleware deal with this
		}
	}
}
