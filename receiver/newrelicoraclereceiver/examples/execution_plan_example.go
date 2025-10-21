package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/internal/metadata"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicoraclereceiver/scrapers"

	_ "github.com/godror/godror" // Oracle driver - more commonly available
)

// Example usage showing how to use both slow queries and execution plan scrapers
func main() {
	// Initialize logger
	logger, _ := zap.NewDevelopment()
	defer logger.Sync()

	// Connect to Oracle database (example connection string)
	// Replace with your actual Oracle connection details
	connStr := "oracle://username:password@hostname:port/service_name"
	db, err := sql.Open("oracle", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to Oracle: %v", err)
	}
	defer db.Close()

	// Test connection
	if err := db.Ping(); err != nil {
		log.Fatalf("Failed to ping Oracle database: %v", err)
	}

	ctx := context.Background()

	// Initialize metrics builder (you'll need to configure this based on your setup)
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()

	// Create proper receiver settings
	settings := receiver.Settings{
		TelemetrySettings: component.TelemetrySettings{
			Logger: logger,
		},
	}
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, settings)

	// Example 1: Using Slow Queries Scraper
	fmt.Println("=== Step 1: Scraping Slow Queries ===")
	slowQueriesScraper, err := scrapers.NewSlowQueriesScraper(db, mb, logger, "oracle-instance", metricsBuilderConfig)
	if err != nil {
		log.Fatalf("Failed to create slow queries scraper: %v", err)
	}

	// Scrape slow queries and get query IDs
	queryIDs, errors := slowQueriesScraper.ScrapeSlowQueries(ctx)
	if len(errors) > 0 {
		logger.Warn("Errors occurred during slow queries scraping", zap.Int("error_count", len(errors)))
		for _, scrapeErr := range errors {
			logger.Error("Scrape error", zap.Error(scrapeErr))
		}
	}

	if len(queryIDs) == 0 {
		fmt.Println("No slow queries found. Exiting...")
		return
	}

	// Example 2: Using Execution Plan Scraper
	fmt.Println("\n=== Step 2: Getting Execution Plans ===")
	executionPlanScraper, err := scrapers.NewExecutionPlanScraper(db, logger)
	if err != nil {
		log.Fatalf("Failed to create execution plan scraper: %v", err)
	}

	// Get execution plans in raw XML format (with detailed logging)
	fmt.Println("\n--- Getting Raw XML Execution Plans ---")
	fmt.Println("Note: This will show detailed logging of the execution plan retrieval process")
	xmlPlans := executionPlanScraper.GetExecutionPlansXML(ctx, queryIDs)

	// Demonstrate raw XML access
	for _, plan := range xmlPlans.Plans {
		if plan.Error == nil && plan.PlanXML != "" {
			fmt.Printf("\n🎯 Raw XML Plan for Query ID %s:\n", plan.QueryID)
			fmt.Printf("   Plan Hash: %s\n", plan.PlanHash)
			fmt.Printf("   Child Number: %d\n", plan.ChildNumber)
			fmt.Printf("   Raw XML Length: %d characters\n", len(plan.PlanXML))
			fmt.Printf("   Raw XML Sample: %s...\n", truncateForDisplay(plan.PlanXML, 200))
		}
	}

	// Get execution plans in text format (human-readable)
	fmt.Println("\n--- Getting Text Execution Plans ---")
	textPlans := executionPlanScraper.GetExecutionPlansText(ctx, queryIDs)

	// Get additional plan details from cache
	fmt.Println("\n--- Getting Plan Cache Details ---")
	cacheDetails := executionPlanScraper.GetPlanDetailsFromCache(ctx, queryIDs)

	// Example 3: Process and save results
	fmt.Println("\n=== Step 3: Processing Results ===")
	processResults(queryIDs, xmlPlans, textPlans, cacheDetails)

	fmt.Println("\n=== Complete! ===")
	fmt.Printf("Total query IDs processed: %d\n", len(queryIDs))
	fmt.Printf("XML plans retrieved: %d/%d\n", xmlPlans.SuccessCount, xmlPlans.TotalCount)
	fmt.Printf("Text plans retrieved: %d/%d\n", textPlans.SuccessCount, textPlans.TotalCount)
	fmt.Printf("Cache details retrieved: %d\n", len(cacheDetails))
}

// Helper function to truncate string for display
func truncateForDisplay(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// processResults demonstrates how you can process and save the execution plans
func processResults(
	queryIDs []string,
	xmlPlans *scrapers.ExecutionPlanResult,
	textPlans *scrapers.ExecutionPlanResult,
	cacheDetails map[string]map[string]interface{},
) {
	fmt.Println("Processing execution plan results...")

	// You can save XML plans to files
	for _, plan := range xmlPlans.Plans {
		if plan.Error == nil && plan.PlanXML != "" {
			filename := fmt.Sprintf("execution_plan_%s.xml", plan.QueryID)
			fmt.Printf("• XML plan for %s would be saved to: %s\n", plan.QueryID, filename)

			// Uncomment to actually save files:
			// err := ioutil.WriteFile(filename, []byte(plan.PlanXML), 0644)
			// if err != nil {
			//     fmt.Printf("  Error saving XML file: %v\n", err)
			// }
		}
	}

	// You can save text plans to files
	for _, plan := range textPlans.Plans {
		if plan.Error == nil && plan.PlanText != "" {
			filename := fmt.Sprintf("execution_plan_%s.txt", plan.QueryID)
			fmt.Printf("• Text plan for %s would be saved to: %s\n", plan.QueryID, filename)

			// Uncomment to actually save files:
			// err := ioutil.WriteFile(filename, []byte(plan.PlanText), 0644)
			// if err != nil {
			//     fmt.Printf("  Error saving text file: %v\n", err)
			// }
		}
	}

	// You can use cache details for analysis
	for queryID, details := range cacheDetails {
		fmt.Printf("• Query %s cache analysis:\n", queryID)
		if executions, ok := details["executions"]; ok {
			fmt.Printf("  - Executed %v times\n", executions)
		}
		if elapsedTime, ok := details["elapsed_time"]; ok {
			fmt.Printf("  - Total elapsed time: %v microseconds\n", elapsedTime)
		}
		if optimizerMode, ok := details["optimizer_mode"]; ok {
			fmt.Printf("  - Optimizer mode: %v\n", optimizerMode)
		}
	}
}
