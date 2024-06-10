package helpers

import (
	"strings"
)

// IsTruthy returns true if a string represents a truthy value
func IsTruthy(val string) bool {
	switch strings.ToLower(strings.TrimSpace(val)) {
	case "y", "yes", "true", "t", "on", "1":
		return true
	default:
		return false
	}
}
