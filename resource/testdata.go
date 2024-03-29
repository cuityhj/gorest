package resource

import (
	"github.com/cuityhj/cement/uuid"
	"github.com/cuityhj/gorest/error"
)

type dumbResource struct {
	ResourceBase
	Number int
}

type DumbHandler struct{}

func (h *DumbHandler) Create(ctx *Context) (Resource, *error.APIError) {
	r := &dumbResource{
		Number: 10,
	}
	r.SetID(uuid.MustGen())
	return r, nil
}

func (h *DumbHandler) Delete(ctx *Context) *error.APIError {
	return nil
}

func (h *DumbHandler) Update(ctx *Context) (Resource, *error.APIError) {
	return &dumbResource{
		Number: 20,
	}, nil
}

func (h *DumbHandler) List(ctx *Context) (interface{}, *error.APIError) {
	return []*dumbResource{&dumbResource{Number: 30}}, nil
}

func (h *DumbHandler) Get(ctx *Context) (Resource, *error.APIError) {
	return &dumbResource{
		Number: 40,
	}, nil
}

func (h *DumbHandler) Action(ctx *Context) (interface{}, *error.APIError) {
	return 50, nil
}
