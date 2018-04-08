package batch

// DataIF is an interface for a data struct, the user-defined, fundamental element of the pipeline.
type DataIF interface {
	GetID() string
}
