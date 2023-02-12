package errors

// EtcdError is struct to categorize errors occurred while processing etcd realted operations
type EtcdError struct {
	Message   string
	operation string
}

func (e *EtcdError) Error() string {
	return e.Message
}

// SnapStoreError is struct to categorize errors occurred while processing snapstore realted operations
type SnapStoreError struct {
	Message   string
	operation string
}

func (e *SnapStoreError) Error() string {
	return e.Message
}

// AnyError checks whether err is nil or not.
func AnyError(err error) bool {
	return err != nil
}
