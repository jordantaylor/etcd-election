package election

type ErrConnectionLost struct{}

func (e ErrConnectionLost) Error() string {
	return "connection to session has been lost"
}

type ErrContextCanceled struct{}

func (e ErrContextCanceled) Error() string {
	return "context was canceled"
}
