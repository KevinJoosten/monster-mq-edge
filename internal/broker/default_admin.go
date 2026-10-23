package broker

import (
	"context"
	"fmt"
	"log/slog"

	"monstermq.io/edge/internal/config"
	"monstermq.io/edge/internal/stores"
	storesqlite "monstermq.io/edge/internal/stores/sqlite"
)

const (
	defaultAdminUsername     = "Admin"
	defaultAdminPassword     = "Admin"
	defaultAnonymousUsername = "Anonymous"
)

func ensureDefaultAdmin(ctx context.Context, cfg *config.Config, storage *stores.Storage, logger *slog.Logger) error {
	if !cfg.UserManagement.Enabled {
		return nil
	}
	user, err := storage.Users.GetUser(ctx, defaultAdminUsername)
	if err != nil {
		return fmt.Errorf("lookup default admin user: %w", err)
	}
	if user != nil {
		logger.Info("default admin user exists", "username", defaultAdminUsername)
		return nil
	}
	hash, err := storesqlite.HashPassword(defaultAdminPassword)
	if err != nil {
		return fmt.Errorf("hash default admin password: %w", err)
	}
	if err := storage.Users.CreateUser(ctx, stores.User{
		Username:     defaultAdminUsername,
		PasswordHash: hash,
		Enabled:      true,
		CanSubscribe: true,
		CanPublish:   true,
		IsAdmin:      true,
	}); err != nil {
		return fmt.Errorf("create default admin user: %w", err)
	}
	logger.Warn("created default admin user; change the default password immediately", "username", defaultAdminUsername)
	return nil
}

func ensureDefaultAnonymous(ctx context.Context, cfg *config.Config, storage *stores.Storage, logger *slog.Logger) error {
	if !cfg.UserManagement.Enabled {
		return nil
	}
	user, err := storage.Users.GetUser(ctx, defaultAnonymousUsername)
	if err != nil {
		return fmt.Errorf("lookup default anonymous user: %w", err)
	}
	if user != nil {
		logger.Info("default anonymous user exists", "username", defaultAnonymousUsername)
		return nil
	}
	if err := storage.Users.CreateUser(ctx, stores.User{
		Username:     defaultAnonymousUsername,
		PasswordHash: "",
		Enabled:      true,
		CanSubscribe: false,
		CanPublish:   false,
		IsAdmin:      false,
	}); err != nil {
		return fmt.Errorf("create default anonymous user: %w", err)
	}
	logger.Info("created default anonymous user for unauthenticated access", "username", defaultAnonymousUsername)
	return nil
}
