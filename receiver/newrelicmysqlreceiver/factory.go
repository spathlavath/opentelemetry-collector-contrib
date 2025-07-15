package newrelicmysqlreceiver

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/newrelicmysqlreceiver/internal/metadata"
)

// NewFactory creates a new receiver factory for New Relic MySQL receiver
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
	)
}

// createMetricsReceiver creates a metrics receiver based on provided config
func createMetricsReceiver(
	ctx context.Context,
	params receiver.Settings,
	cfg component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {

	receiverCfg := cfg.(*Config)

	// Create the scraper
	s, err := newMySQLScraper(params, receiverCfg)
	if err != nil {
		return nil, err
	}

	// Create the scraper controller
	return scraperhelper.NewMetricsController(
		&receiverCfg.ControllerConfig,
		params,
		consumer,
		scraperhelper.AddScraper(metadata.Type, s),
	)
}

// newMySQLScraper creates a new MySQL scraper
func newMySQLScraper(
	params receiver.Settings,
	cfg *Config,
) (scraper.Metrics, error) {

	// Initialize metrics builder
	metricsBuilderConfig := metadata.DefaultMetricsBuilderConfig()
	mb := metadata.NewMetricsBuilder(metricsBuilderConfig, params)

	// Initialize resource builder
	resourceConfig := metadata.DefaultResourceAttributesConfig()
	rb := metadata.NewResourceBuilder(resourceConfig)

	// Create the scraper implementation
	s := &mySQLScraper{
		config: cfg,
		logger: params.Logger,
		mb:     mb,
		rb:     rb,
	}

	// Return the scraper wrapped with helper functions
	return scraper.NewMetrics(
		s.scrape,
		scraper.WithStart(s.start),
		scraper.WithShutdown(s.shutdown),
	)
}
