package run

import "context"

type runContextKeyType int

const currentRunKey runContextKeyType = iota

// FromContext extracts the current Run from a context.
//
// If no Run is currently set in ctx an implementation of a Run that performs no operations is returned.
func FromContext(ctx context.Context) Run {
	if ctx == nil {
		return &noopRun{}
		// return noopSpanInstance
	}
	if r, ok := ctx.Value(currentRunKey).(Run); ok {
		return r
	}

	return &noopRun{}
}

// ContextWithRun returns a copy of the Context with the Run saved.
func ContextWithRun(parent context.Context, run Run) context.Context {
	return context.WithValue(parent, currentRunKey, run)
}
