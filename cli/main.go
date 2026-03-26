// CLI for the Judgment Labs metrics pipeline.
//
// Usage:
//
//	jl metrics list [--service <name>] [--metric <name>] [--since <dur>] [--limit <n>] [--json]
//	jl metrics inspect <metric-name> [--service <name>] [--since <dur>] [--json]
//	jl rules   list [--json]
//	jl rules   create --name <n> --metric <m> [--service <s>] --operator <op> --threshold <t> [--window <dur>] [--action <type>] [--json]
//	jl rules   delete <name>
//	jl alerts  list [--rule <name>] [--since <dur>] [--status firing|resolved] [--json]
//	jl alerts  inspect <alert-id> [--json]
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/yarivkenan/JL/cli/internal/client"
	"github.com/yarivkenan/JL/cli/internal/format"
)

const defaultServer = "http://localhost:8081"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}

	serverURL := os.Getenv("JL_SERVER")
	if serverURL == "" {
		serverURL = defaultServer
	}
	c := client.New(serverURL)
	ctx := context.Background()

	resource := os.Args[1]
	subArgs := os.Args[2:]

	var err error
	switch resource {
	case "metrics":
		err = metricsCmd(ctx, c, subArgs)
	case "rules":
		err = rulesCmd(ctx, c, subArgs)
	case "alerts":
		err = alertsCmd(ctx, c, subArgs)
	case "help", "--help", "-h":
		usage()
		return
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n\n", resource)
		usage()
		os.Exit(1)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprint(os.Stderr, `Usage: jl <command> <subcommand> [flags]

Commands:
  metrics list      List metrics
  metrics inspect   Inspect a metric's recent data points
  rules   list      List all rules
  rules   create    Create a new rule
  rules   delete    Delete a rule by name
  alerts  list      List alerts
  alerts  inspect   Inspect a single alert

Environment:
  JL_SERVER    Query service URL (default: http://localhost:8081)
`)
}

// --- Metrics ----------------------------------------------------------------

func metricsCmd(ctx context.Context, c *client.Client, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: jl metrics <list|inspect>")
	}
	switch args[0] {
	case "list":
		return metricsList(ctx, c, args[1:])
	case "inspect":
		return metricsInspect(ctx, c, args[1:])
	default:
		return fmt.Errorf("unknown metrics subcommand %q", args[0])
	}
}

func metricsList(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("metrics list", flag.ExitOnError)
	service := fs.String("service", "", "Filter by service name")
	metric := fs.String("metric", "", "Filter by metric name")
	since := fs.String("since", "", `Time filter, e.g. "1h", "30m"`)
	limit := fs.Int("limit", 50, "Max results")
	asJSON := fs.Bool("json", false, "Output as JSON")
	fs.Parse(args)

	// If --metric is set, we can use the name filter directly on /v1/metrics.
	// If --service or --since is set, we need to query data_points instead to get
	// a distinct list of metric names seen from that service/window.
	if *service != "" || *since != "" {
		// Query data points and extract unique metrics.
		points, err := c.ListDataPoints(ctx, client.DataPointsListOpts{
			ServiceName: *service,
			MetricName:  *metric,
			Since:       *since,
			Limit:       *limit,
		})
		if err != nil {
			return err
		}
		if *asJSON {
			return printJSON(points)
		}
		seen := map[string]bool{}
		tbl := format.NewTable(os.Stdout, "METRIC", "SERVICE", "DATA POINTS")
		counts := map[string]int{}
		services := map[string]string{}
		for _, p := range points {
			counts[p.MetricName]++
			services[p.MetricName] = p.ServiceName
		}
		for name, cnt := range counts {
			if !seen[name] {
				seen[name] = true
				tbl.Row(name, services[name], strconv.Itoa(cnt))
			}
		}
		tbl.Flush()
		return nil
	}

	metrics, err := c.ListMetrics(ctx, client.MetricsListOpts{Name: *metric})
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(metrics)
	}
	tbl := format.NewTable(os.Stdout, "NAME", "TYPE", "UNIT", "DESCRIPTION")
	for _, m := range metrics {
		tbl.Row(m.Name, m.Type, m.Unit, m.Description)
	}
	tbl.Flush()
	return nil
}

func metricsInspect(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("metrics inspect", flag.ExitOnError)
	service := fs.String("service", "", "Filter by service name")
	since := fs.String("since", "1h", `Time window, e.g. "1h", "30m"`)
	asJSON := fs.Bool("json", false, "Output as JSON")
	fs.Parse(args)

	metricName := fs.Arg(0)
	if metricName == "" {
		return fmt.Errorf("usage: jl metrics inspect <metric-name>")
	}

	// Fetch metric definition.
	metrics, err := c.ListMetrics(ctx, client.MetricsListOpts{Name: metricName})
	if err != nil {
		return err
	}
	if len(metrics) == 0 {
		return fmt.Errorf("metric %q not found", metricName)
	}
	m := metrics[0]

	// Fetch recent data points.
	points, err := c.ListDataPoints(ctx, client.DataPointsListOpts{
		MetricName:  metricName,
		ServiceName: *service,
		Since:       *since,
		Limit:       100,
	})
	if err != nil {
		return err
	}

	if *asJSON {
		return printJSON(map[string]any{"metric": m, "data_points": points})
	}

	format.KV(os.Stdout,
		"Name", m.Name,
		"Type", m.Type,
		"Unit", m.Unit,
		"Description", m.Description,
	)
	fmt.Printf("\nRecent data points (%d):\n\n", len(points))
	tbl := format.NewTable(os.Stdout, "TIMESTAMP", "VALUE", "SERVICE", "ATTRIBUTES")
	for _, p := range points {
		attrs := fmtMap(p.Attributes)
		tbl.Row(p.Timestamp.Format("2006-01-02 15:04:05"), fmt.Sprintf("%.4f", p.Value), p.ServiceName, attrs)
	}
	tbl.Flush()
	return nil
}

// --- Rules ------------------------------------------------------------------

func rulesCmd(ctx context.Context, c *client.Client, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: jl rules <list|create|delete>")
	}
	switch args[0] {
	case "list":
		return rulesList(ctx, c, args[1:])
	case "create":
		return rulesCreate(ctx, c, args[1:])
	case "delete":
		return rulesDelete(ctx, c, args[1:])
	default:
		return fmt.Errorf("unknown rules subcommand %q", args[0])
	}
}

func rulesList(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("rules list", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "Output as JSON")
	fs.Parse(args)

	rules, err := c.ListRules(ctx)
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(rules)
	}
	tbl := format.NewTable(os.Stdout, "NAME", "METRIC", "CONDITION", "WINDOW", "ACTION")
	for _, r := range rules {
		cond := fmtCondition(r.Condition)
		window := fmtWindow(r.Window)
		action := fmtAction(r.Action)
		tbl.Row(r.Name, r.MetricName, cond, window, action)
	}
	tbl.Flush()
	return nil
}

func rulesCreate(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("rules create", flag.ExitOnError)
	name := fs.String("name", "", "Rule name (required)")
	metricName := fs.String("metric", "", "Metric name to evaluate (required)")
	service := fs.String("service", "", "Service name filter")
	operator := fs.String("operator", ">", "Comparison operator (>, >=, <, <=, ==)")
	threshold := fs.Float64("threshold", 0, "Threshold value (required)")
	aggregation := fs.String("aggregation", "avg", "Aggregation function (avg, p95, count, value)")
	window := fs.String("window", "5m", "Time window duration")
	action := fs.String("action", "alert", "Action type")
	severity := fs.String("severity", "warning", "Alert severity (info, warning, critical)")
	message := fs.String("message", "", "Alert message")
	asJSON := fs.Bool("json", false, "Output as JSON")
	fs.Parse(args)

	if *name == "" || *metricName == "" {
		return fmt.Errorf("--name and --metric are required")
	}

	rule := &client.Rule{
		Name:        *name,
		MetricName:  *metricName,
		ServiceName: *service,
		Condition: map[string]any{
			"aggregation": *aggregation,
			"operator":    *operator,
			"threshold":   *threshold,
		},
		Window: map[string]any{
			"type":     "time",
			"duration": *window,
		},
		Action: map[string]any{
			"type":     *action,
			"severity": *severity,
			"message":  *message,
		},
	}

	created, err := c.CreateRule(ctx, rule)
	if err != nil {
		return err
	}

	if *asJSON {
		return printJSON(created)
	}
	fmt.Printf("Rule %q created.\n", created.Name)
	return nil
}

func rulesDelete(ctx context.Context, c *client.Client, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: jl rules delete <name>")
	}
	name := args[0]
	if err := c.DeleteRule(ctx, name); err != nil {
		return err
	}
	fmt.Printf("Rule %q deleted.\n", name)
	return nil
}

// --- Alerts -----------------------------------------------------------------

func alertsCmd(ctx context.Context, c *client.Client, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: jl alerts <list|inspect>")
	}
	switch args[0] {
	case "list":
		return alertsList(ctx, c, args[1:])
	case "inspect":
		return alertsInspect(ctx, c, args[1:])
	default:
		return fmt.Errorf("unknown alerts subcommand %q", args[0])
	}
}

func alertsList(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("alerts list", flag.ExitOnError)
	rule := fs.String("rule", "", "Filter by rule name")
	since := fs.String("since", "", `Time filter, e.g. "1h", "30m"`)
	status := fs.String("status", "", "Filter by status (firing|resolved)")
	asJSON := fs.Bool("json", false, "Output as JSON")
	fs.Parse(args)

	alerts, err := c.ListAlerts(ctx, client.AlertsListOpts{
		Rule:   *rule,
		Status: *status,
		Since:  *since,
	})
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(alerts)
	}
	tbl := format.NewTable(os.Stdout, "ID", "RULE", "METRIC", "VALUE", "THRESHOLD", "STATUS", "TIMESTAMP")
	for _, a := range alerts {
		tbl.Row(
			short(a.ID),
			a.RuleName,
			a.MetricName,
			fmt.Sprintf("%.2f", a.EvaluatedValue),
			fmt.Sprintf("%s %.2f", a.Operator, a.Threshold),
			a.Status,
			a.CreatedAt.Format("2006-01-02 15:04:05"),
		)
	}
	tbl.Flush()
	return nil
}

func alertsInspect(ctx context.Context, c *client.Client, args []string) error {
	fs := flag.NewFlagSet("alerts inspect", flag.ExitOnError)
	asJSON := fs.Bool("json", false, "Output as JSON")
	fs.Parse(args)

	id := fs.Arg(0)
	if id == "" {
		return fmt.Errorf("usage: jl alerts inspect <alert-id>")
	}

	alert, err := c.GetAlert(ctx, id)
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(alert)
	}

	resolvedAt := "—"
	if alert.ResolvedAt != nil {
		resolvedAt = alert.ResolvedAt.Format("2006-01-02 15:04:05")
	}

	format.KV(os.Stdout,
		"ID", alert.ID,
		"Rule", alert.RuleName,
		"Metric", alert.MetricName,
		"Service", alert.ServiceName,
		"Value", fmt.Sprintf("%.4f", alert.EvaluatedValue),
		"Operator", alert.Operator,
		"Threshold", fmt.Sprintf("%.4f", alert.Threshold),
		"Status", alert.Status,
		"Created", alert.CreatedAt.Format("2006-01-02 15:04:05"),
		"Resolved", resolvedAt,
	)
	return nil
}

// --- Formatting helpers -----------------------------------------------------

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}

func fmtMap(m map[string]any) string {
	if len(m) == 0 {
		return ""
	}
	parts := make([]string, 0, len(m))
	for k, v := range m {
		parts = append(parts, fmt.Sprintf("%s=%v", k, v))
	}
	return strings.Join(parts, ", ")
}

func fmtCondition(c map[string]any) string {
	agg, _ := c["aggregation"].(string)
	op, _ := c["operator"].(string)
	th, _ := c["threshold"].(float64)
	return fmt.Sprintf("%s %s %.0f", agg, op, th)
}

func fmtWindow(w map[string]any) string {
	d, _ := w["duration"].(string)
	if d == "" {
		return "—"
	}
	return d
}

func fmtAction(a map[string]any) string {
	t, _ := a["type"].(string)
	sev, _ := a["severity"].(string)
	if sev != "" {
		return t + " (" + sev + ")"
	}
	return t
}
