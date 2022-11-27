package user

import "rest/pkg/logging"

type Service struct {
	storage Storage
	logger  *logging.Logger
}
