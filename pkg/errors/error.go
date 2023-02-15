package errors

// EtcdError is struct to categorize errors occurred while processing etcd related operations
type EtcdError struct {
	Message   string
	operation string
}

func (e *EtcdError) Error() string {
	return e.Message
}

// StoreError is struct to categorize errors occurred while processing snap store related operations
type StoreError struct {
	Message   string
	operation string
}

func (e *StoreError) Error() string {
	return e.Message
}

// AnyError checks whether err is nil or not.
func AnyError(err error) bool {
	return err != nil
}
