package bullhorn

import "fmt"

type Error struct {
	StatusCode  int
	RequestPath string
	Message     string
}

func (e *Error) Error() string {
	msg := fmt.Sprintf("Bullhorn error: %s", e.Message)
	extra := fmt.Sprintf("got response code %d for request path %q", e.StatusCode, e.RequestPath)

	return msg + " " + extra
}
