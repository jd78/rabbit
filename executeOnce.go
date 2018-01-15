package rabbit

type executeOnce struct {
	executed bool
}

func (e *executeOnce) MaybeExecute(f func()) {
	if !e.executed {
		f()
		e.executed = true
	}
}
