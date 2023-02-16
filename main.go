package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

func main() {
	if err := run(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

type Service struct {
	cloudwatchlogs *cloudwatchlogs.Client
	streams        map[string]string
}

var refreshRate = time.Second * 3

func run() error {
	ctx := context.Background()
	var err error
	s := &Service{}
	s.cloudwatchlogs, err = initCloudWatchClient(ctx)
	if err != nil {
		return err
	}

	if err := s.updateStreams(ctx); err != nil {
		return err
	}

	ticker := time.NewTicker(refreshRate)
	for group, stream := range s.streams {
		go func(group, stream string) {
			for {
				select {
				case <-ticker.C:
					logs, err := s.fetchStreamLogs(ctx, group, stream)
					if err != nil {
						fmt.Printf("failed to fetch logs for stream %s, err: %s\n", stream, err)
					}

					for _, log := range logs {
						fmt.Println(log)
					}
				}
			}
		}(group, stream)
	}

	select {}
	return nil
}

func (s *Service) fetchStreamLogs(ctx context.Context, group, stream string) ([]string, error) {
	fmt.Println("fetching logs for stream: ", stream, " in group: ", group)
	// end := time.Now()
	// start := end.Add(-refreshRate)
	// endUnix := end.Unix()
	// startUnix := start.Unix()
	out, err := s.cloudwatchlogs.GetLogEvents(ctx, &cloudwatchlogs.GetLogEventsInput{
		LogStreamName: &stream,
		LogGroupName:  &group,
	})
	if err != nil {
		return nil, err
	}

	records := make([]string, 0)
	for _, e := range out.Events {
		records = append(records, *e.Message)
	}

	return records, nil
}

func (s *Service) logStream(ctx context.Context, group string) (string, error) {
	limit := int32(1)
	descending := true
	out, err := s.cloudwatchlogs.DescribeLogStreams(ctx, &cloudwatchlogs.DescribeLogStreamsInput{
		Descending:   &descending,
		OrderBy:      types.OrderByLastEventTime,
		Limit:        &limit,
		LogGroupName: &group,
	})
	if err != nil {
		return "", fmt.Errorf("failed to describe log group streams: %w", err)
	}

	if len(out.LogStreams) == 0 {
		return "", fmt.Errorf("no log streams found for group: %s", group)
	}

	return *out.LogStreams[0].LogStreamName, nil
}

func (s *Service) updateStreams(ctx context.Context) error {
	groups := flag.String("g", "", "define multiple log groups separated by comma")
	flag.Parse()

	if groups == nil {
		return errors.New("-g log groups flag is not set")
	}

	s.streams = make(map[string]string)
	for _, group := range strings.Split(*groups, ",") {
		if group == "" {
			return errors.New("log group cannot be empty")
		}

		stream, err := s.logStream(ctx, group)
		if err != nil {
			return err
		}

		s.streams[group] = stream
	}

	return nil
}

func initCloudWatchClient(ctx context.Context) (*cloudwatchlogs.Client, error) {
	cfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}

	return cloudwatchlogs.NewFromConfig(cfg), nil
}
