package geckoboard

import "fmt"

type Error struct {
	Detail     `json:"error"`
	StatusCode int `json:"-"`
}

type Detail struct {
	Message string `json:"message"`
}

func (e Error) Error() string {
	template := "There was an error sending the data to Geckoboard's API: %q: with response code %d"
	return fmt.Sprintf(template, e.Detail.Message, e.StatusCode)
}
