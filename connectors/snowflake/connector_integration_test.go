//go:build integration
// +build integration

package snowflake

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/centralmind/gateway/connectors"
	"github.com/centralmind/gateway/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/data.yaml
var dataYaml []byte

func TestConnector_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Use real Snowflake credentials
	cfg := Config{
		Account:   getEnvOrDefault("SNOWFLAKE_TEST_ACCOUNT", "YHFCEQW-VS84270"),
		Database:  getEnvOrDefault("SNOWFLAKE_TEST_DATABASE", "GOCI"),
		User:      getEnvOrDefault("SNOWFLAKE_TEST_USER", "GATEWAYCI"),
		Password:  getEnvOrDefault("SNOWFLAKE_TEST_PASSWORD", "gatewayCI406PM1"),
		Warehouse: getEnvOrDefault("SNOWFLAKE_TEST_WAREHOUSE", "COMPUTE_WH"),
		Schema:    getEnvOrDefault("SNOWFLAKE_TEST_SCHEMA", "PUBLIC"),
		Role:      getEnvOrDefault("SNOWFLAKE_TEST_ROLE", "ACCOUNTADMIN"),
	}

	// Create connector
	var connector connectors.Connector
	connector, err := connectors.New(cfg.Type(), cfg)
	require.NoError(t, err)

	// Test connection first
	err = connector.Ping(ctx)
	if err != nil {
		// Check if the error is due to expired password
		errStr := err.Error()
		if contains(errStr, "390106") && contains(errStr, "expired") {
			t.Skip("Snowflake password has expired, skipping integration test")
		}
		t.Fatalf("Failed to connect to Snowflake: %v", err)
	}

	// Setup and cleanup test data
	setupTestData(t, cfg)
	defer cleanupTestData(t, cfg)

	t.Run("ping", func(t *testing.T) {
		err := connector.Ping(ctx)
		assert.NoError(t, err)
	})

	t.Run("discovery", func(t *testing.T) {
		tables, err := connector.Discovery(ctx, nil)
		require.NoError(t, err)

		// Check if our test table exists
		var found bool
		for _, table := range tables {
			if table.Name == "INTEGRATION_TEST_USERS" {
				found = true
				assert.Greater(t, table.RowCount, 0)

				// Verify columns
				expectedColumns := map[string]model.ColumnType{
					"ID":         model.TypeInteger,
					"NAME":       model.TypeString,
					"EMAIL":      model.TypeString,
					"AGE":        model.TypeInteger,
					"SALARY":     model.TypeNumber,
					"IS_ACTIVE":  model.TypeBoolean,
					"CREATED_AT": model.TypeDatetime,
					"METADATA":   model.TypeObject,
					"SKILLS":     model.TypeArray,
				}

				for _, col := range table.Columns {
					expectedType, ok := expectedColumns[col.Name]
					assert.True(t, ok, "unexpected column: %s", col.Name)
					assert.Equal(t, expectedType, col.Type, "column %s has unexpected type", col.Name)
				}
				break
			}
		}
		assert.True(t, found, "test table not found in discovery")
	})

	t.Run("discovery_specific_tables", func(t *testing.T) {
		tables, err := connector.Discovery(ctx, []string{"INTEGRATION_TEST_USERS", "INTEGRATION_TEST_ORDERS"})
		require.NoError(t, err)

		// Should return exactly the requested tables
		tableNames := make([]string, len(tables))
		for i, table := range tables {
			tableNames[i] = table.Name
		}
		assert.ElementsMatch(t, []string{"INTEGRATION_TEST_USERS", "INTEGRATION_TEST_ORDERS"}, tableNames)
	})

	t.Run("query_with_params", func(t *testing.T) {
		selectQuery := `
			SELECT ID, NAME, EMAIL, AGE
			FROM INTEGRATION_TEST_USERS
			WHERE ID = :user_id
		`

		params := map[string]any{
			"user_id": 1,
		}

		results, err := connector.Query(
			ctx,
			model.Endpoint{
				Query: selectQuery,
				Params: []model.EndpointParams{
					{
						Name: "user_id",
						Type: string(model.TypeInteger),
					},
				},
			},
			params,
		)
		require.NoError(t, err)
		require.Len(t, results, 1)

		row := results[0]
		assert.Equal(t, "1", row["ID"])
		assert.Equal(t, "Alice Johnson", row["NAME"])
		assert.Equal(t, "alice@example.com", row["EMAIL"])
		assert.Equal(t, "30", row["AGE"])
	})

	t.Run("sample", func(t *testing.T) {
		samples, err := connector.Sample(ctx, model.Table{Name: "INTEGRATION_TEST_USERS"})
		require.NoError(t, err)
		assert.LessOrEqual(t, len(samples), 5) // Sample should return at most 5 rows
		assert.Greater(t, len(samples), 0)     // But at least 1 row
	})

	t.Run("query_with_limit_offset", func(t *testing.T) {
		// Snowflake doesn't support parameters in LIMIT/OFFSET clauses, so we use fixed values
		selectQuery := `
			SELECT ID, NAME
			FROM INTEGRATION_TEST_USERS
			ORDER BY ID
			LIMIT 2
			OFFSET 1
		`

		results, err := connector.Query(
			ctx,
			model.Endpoint{
				Query:  selectQuery,
				Params: []model.EndpointParams{},
			},
			map[string]any{},
		)
		require.NoError(t, err)
		require.Len(t, results, 2)

		// Should get second and third users due to OFFSET 1
		assert.Equal(t, "2", results[0]["ID"])
		assert.Equal(t, "3", results[1]["ID"])
	})

	t.Run("query_complex_types", func(t *testing.T) {
		selectQuery := `
			SELECT 
				NAME,
				METADATA,
				SKILLS
			FROM INTEGRATION_TEST_USERS
			WHERE ID = :user_id
		`

		params := map[string]any{
			"user_id": 1,
		}

		results, err := connector.Query(
			ctx,
			model.Endpoint{
				Query: selectQuery,
				Params: []model.EndpointParams{
					{
						Name: "user_id",
						Type: string(model.TypeInteger),
					},
				},
			},
			params,
		)
		require.NoError(t, err)
		require.Len(t, results, 1)

		row := results[0]
		assert.NotNil(t, row["METADATA"])
		assert.NotNil(t, row["SKILLS"])
	})

	// t.Run("infer_query_columns", func(t *testing.T) {
	// 	query := `
	// 		SELECT
	// 			ID,
	// 			NAME,
	// 			AGE,
	// 			SALARY,
	// 			IS_ACTIVE,
	// 			CREATED_AT
	// 		FROM INTEGRATION_TEST_USERS
	// 		WHERE ID > 0
	// 	`

	// 	columns, err := connector.InferQuery(ctx, query)
	// 	require.NoError(t, err)
	// 	require.Greater(t, len(columns), 0)

	// 	// Verify column types are correctly inferred
	// 	columnTypes := make(map[string]model.ColumnType)
	// 	for _, col := range columns {
	// 		columnTypes[col.Name] = col.Type
	// 	}

	// 	assert.Equal(t, model.TypeInteger, columnTypes["ID"])
	// 	assert.Equal(t, model.TypeString, columnTypes["NAME"])
	// 	assert.Equal(t, model.TypeInteger, columnTypes["AGE"])
	// 	assert.Equal(t, model.TypeNumber, columnTypes["SALARY"])
	// 	assert.Equal(t, model.TypeBoolean, columnTypes["IS_ACTIVE"])
	// 	assert.Equal(t, model.TypeDatetime, columnTypes["CREATED_AT"])
	// })
}

func getEnvOrDefault(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func setupTestData(t *testing.T, cfg Config) {
	dsn, err := cfg.MakeDSN()
	require.NoError(t, err)

	db, err := sql.Open("snowflake", dsn)
	require.NoError(t, err)
	defer db.Close()

	// Set session parameters
	_, err = db.Exec(fmt.Sprintf("USE DATABASE %s", cfg.Database))
	if err != nil {
		t.Logf("Warning: Failed to use database: %v", err)
	}

	_, err = db.Exec(fmt.Sprintf("USE SCHEMA %s", cfg.Schema))
	if err != nil {
		t.Logf("Warning: Failed to use schema: %v", err)
	}

	// Create test tables
	queries := []string{
		// Drop tables if they exist
		`DROP TABLE IF EXISTS INTEGRATION_TEST_USERS`,
		`DROP TABLE IF EXISTS INTEGRATION_TEST_ORDERS`,

		// Create users table with various data types
		`CREATE TABLE INTEGRATION_TEST_USERS (
			ID INTEGER PRIMARY KEY,
			NAME VARCHAR(100),
			EMAIL VARCHAR(255),
			AGE INTEGER,
			SALARY DECIMAL(10,2),
			IS_ACTIVE BOOLEAN,
			CREATED_AT TIMESTAMP,
			METADATA VARIANT,
			SKILLS ARRAY
		)`,

		// Create orders table
		`CREATE TABLE INTEGRATION_TEST_ORDERS (
			ID INTEGER PRIMARY KEY,
			USER_ID INTEGER,
			ORDER_DATE DATE,
			TOTAL DECIMAL(10,2),
			STATUS VARCHAR(50)
		)`,

		// Insert test data into users
		`INSERT INTO INTEGRATION_TEST_USERS 
		SELECT 
			column1 AS ID,
			column2 AS NAME,
			column3 AS EMAIL,
			column4 AS AGE,
			column5 AS SALARY,
			column6 AS IS_ACTIVE,
			column7 AS CREATED_AT,
			PARSE_JSON(column8) AS METADATA,
			PARSE_JSON(column9) AS SKILLS
		FROM VALUES
			(1, 'Alice Johnson', 'alice@example.com', 30, 75000.50, true, '2023-01-15 10:30:00', 
			 '{"department": "Engineering", "level": "Senior"}', 
			 '["Go", "Python", "SQL"]'),
			(2, 'Bob Smith', 'bob@example.com', 25, 60000.00, true, '2023-02-20 14:45:00',
			 '{"department": "Sales", "level": "Junior"}',
			 '["Excel", "PowerPoint"]'),
			(3, 'Charlie Brown', 'charlie@example.com', 35, 85000.75, false, '2023-03-10 09:15:00',
			 '{"department": "Marketing", "level": "Manager"}',
			 '["SEO", "Content Marketing"]'),
			(4, 'Diana Ross', 'diana@example.com', 28, 70000.25, true, '2023-04-05 16:20:00',
			 '{"department": "Engineering", "level": "Mid"}',
			 '["JavaScript", "React", "Node.js"]'),
			(5, 'Edward Norton', 'edward@example.com', 45, 120000.00, true, '2023-05-01 11:00:00',
			 '{"department": "Executive", "level": "Director"}',
			 '["Leadership", "Strategy"]')`,

		// Insert test data into orders
		`INSERT INTO INTEGRATION_TEST_ORDERS VALUES
			(1, 1, '2023-06-01', 150.50, 'completed'),
			(2, 1, '2023-06-15', 225.75, 'completed'),
			(3, 2, '2023-06-10', 89.99, 'pending'),
			(4, 3, '2023-06-20', 450.00, 'cancelled'),
			(5, 4, '2023-06-25', 320.25, 'completed')`,
	}

	for _, query := range queries {
		_, err := db.Exec(query)
		if err != nil {
			t.Fatalf("Failed to execute query: %s\nError: %v", query, err)
		}
	}
}

func cleanupTestData(t *testing.T, cfg Config) {
	dsn, err := cfg.MakeDSN()
	if err != nil {
		t.Logf("Failed to create DSN for cleanup: %v", err)
		return
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		t.Logf("Failed to open connection for cleanup: %v", err)
		return
	}
	defer db.Close()

	// Drop test tables
	queries := []string{
		`DROP TABLE IF EXISTS INTEGRATION_TEST_USERS`,
		`DROP TABLE IF EXISTS INTEGRATION_TEST_ORDERS`,
	}

	for _, query := range queries {
		_, err := db.Exec(query)
		if err != nil {
			t.Logf("Failed to cleanup: %s\nError: %v", query, err)
		}
	}
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
