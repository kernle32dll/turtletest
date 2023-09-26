package persistence

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/mariadb"
)

type dbConfig struct {
	address  string
	port     int
	username string
	password string
}

// MariaDBContainer is an abstraction over the mariadb testcontainers-go module, which creates
// a MariaDB container once, but allows creation of a new database for each test.
type MariaDBContainer struct {
	container    *mariadb.MariaDBContainer
	databaseName string
	dbConfig     dbConfig
}

// NewMariaDBContainer creates a new MariaDBContainer.
func NewMariaDBContainer(t testing.TB, opts ...testcontainers.ContainerCustomizer) *MariaDBContainer {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg, container := startMariaDBContainer(t, ctx, opts...)

	return &MariaDBContainer{
		container:    container,
		databaseName: "",
		dbConfig:     cfg,
	}
}

// ShutdownContainer shuts down the database container.
func (s *MariaDBContainer) ShutdownContainer(t testing.TB, ctx context.Context) {
	t.Helper()

	t.Log("Shutting down database container")
	shutdownCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	if err := s.container.Terminate(shutdownCtx); err != nil {
		t.Logf("Failed to shutdown container: %s", err)
	}
}

// CreateDatabase ensures that a new database is created.
func (s *MariaDBContainer) CreateDatabase(t testing.TB, ctx context.Context) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	// Generate a new, unique database name
	s.databaseName = uuid.NewString()

	s.ensureDatabase(t, ctx)
}

// RemoveDatabase removes the database created by CreateDatabase.
func (s *MariaDBContainer) RemoveDatabase(t testing.TB, ctx context.Context) {
	t.Helper()

	ctx, cancel := context.WithTimeout(ctx, 1*time.Minute)
	defer cancel()

	s.removeDatabase(t, ctx)
}

func (s *MariaDBContainer) removeDatabase(t testing.TB, ctx context.Context) {
	t.Helper()

	t.Logf("Removing database %q", s.databaseName)

	db := s.rootConnect(t, ctx)
	defer func() {
		if err := db.Close(); err != nil {
			// Not breaking, just informal
			t.Logf("Failed to close sql connection for DB removal: %s", err)
		}
	}()

	if _, err := db.ExecContext(ctx, fmt.Sprintf("DROP DATABASE `%s`", s.databaseName)); err != nil {
		// Not breaking, just informal
		t.Logf("Failed to delete database %q: %s", s.databaseName, err)
	}
}

func (s *MariaDBContainer) ensureDatabase(t testing.TB, ctx context.Context) {
	t.Helper()

	t.Logf("Creating database %q", s.databaseName)

	db := s.rootConnect(t, ctx)
	defer func() {
		if err := db.Close(); err != nil {
			// Not breaking, just informal
			t.Logf("Failed to close sql connection for DB creation: %s", err)
		}
	}()

	tx, err := db.Begin()
	require.NoError(t, err)

	_, err = tx.Exec(fmt.Sprintf("CREATE DATABASE `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci", s.databaseName))
	require.NoError(t, err)
	_, err = tx.Exec(fmt.Sprintf("GRANT ALL PRIVILEGES ON `%s`.* to `%s`@'%%';", s.databaseName, s.dbConfig.username))
	require.NoError(t, err)

	require.NoError(t, tx.Commit())
}

func (s *MariaDBContainer) rootConnect(t testing.TB, ctx context.Context) *sqlx.DB {
	t.Helper()

	conString := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/?parseTime=true&multiStatements=true",
		"root",
		s.dbConfig.password,
		s.dbConfig.address,
		s.dbConfig.port,
	)

	db, err := sqlx.ConnectContext(ctx, "mysql", conString)
	require.NoError(t, err, "Failed to connect to database as root")

	return db
}

// Connect establishes a sqlx.DB connection to the database.
func (s *MariaDBContainer) Connect(t testing.TB, ctx context.Context) *sqlx.DB {
	t.Helper()

	conString := fmt.Sprintf(
		"%s:%s@tcp(%s:%d)/%s?parseTime=true&multiStatements=true",
		s.dbConfig.username,
		s.dbConfig.password,
		s.dbConfig.address,
		s.dbConfig.port,
		s.databaseName,
	)

	db, err := sqlx.ConnectContext(ctx, "mysql", conString)
	require.NoError(t, err, "Failed to connect to database")

	return db
}

func startMariaDBContainer(t testing.TB, ctx context.Context, opts ...testcontainers.ContainerCustomizer) (dbConfig, *mariadb.MariaDBContainer) {
	t.Helper()

	username := ""
	password := ""

	t.Logf("Starting MariaDB database container")

	mariadbContainer, err := mariadb.Run(ctx, "mariadb:11.4",
		append(
			append(
				// prepend logger config, but allow overriding
				[]testcontainers.ContainerCustomizer{
					testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
						req.Logger = testcontainers.TestLogger(t)
						return nil
					}),
				},
				opts...,
			),
			// Capture username and password, since these are not exported via mariadb.MariaDBContainer
			testcontainers.CustomizeRequestOption(func(req *testcontainers.GenericContainerRequest) error {
				req.LifecycleHooks = append(req.LifecycleHooks, testcontainers.ContainerLifecycleHooks{
					PreCreates: []testcontainers.ContainerRequestHook{
						func(ctx context.Context, req testcontainers.ContainerRequest) error {
							username = req.Env["MARIADB_USER"]
							password = req.Env["MARIADB_PASSWORD"]
							return nil
						},
					},
				})
				return nil
			}),
		)...,
	)
	require.NoError(t, err, "Database container could not be started")

	host, err := mariadbContainer.Host(ctx)
	require.NoError(t, err, "Failed to resolve container host")

	port, err := mariadbContainer.MappedPort(ctx, "3306/tcp")
	require.NoError(t, err, "Failed to resolve container port")

	return dbConfig{
		address:  host,
		port:     port.Int(),
		username: username,
		password: password,
	}, mariadbContainer
}
