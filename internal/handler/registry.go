package handler

import (
	"fmt"
	"log/slog"
)

type Registry struct {
	handlers map[string]EventHandler
	active   []string
}

func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]EventHandler),
	}
}

func (r *Registry) Register(h EventHandler) {
	r.handlers[h.Name()] = h
}

func (r *Registry) Activate(names []string) {
	r.active = names
}

func (r *Registry) GetActive() []EventHandler {
	var active []EventHandler
	for _, name := range r.active {
		if h, ok := r.handlers[name]; ok {
			active = append(active, h)
		}
	}
	return active
}

func (r *Registry) CloseAll(log *slog.Logger) {
	for _, h := range r.GetActive() {
		if err := h.Close(); err != nil {
			log.Error("failed to close handler", "handler", h.Name(), "error", err)
		}
	}
}

func (r *Registry) ValidateConfig(availableHandlers []string) error {
	for _, name := range r.active {
		if _, ok := r.handlers[name]; !ok {
			return fmt.Errorf("handler %q is activated but not registered (available: %v)", name, availableHandlers)
		}
	}
	return nil
}
