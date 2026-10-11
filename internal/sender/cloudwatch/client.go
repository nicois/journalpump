// Copyright 2026, Aiven, https://aiven.io/
//
// This file is under the Apache License, Version 2.0.
// See the file `LICENSE` for details.

package cloudwatch

import (
	"context"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwltypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/smithy-go"
)

// sdkClient wraps the aws-sdk-go-v2 cloudwatchlogs client behind logsClient.
// All calls take ctx, so cancellation and deadlines propagate (the SDK adds
// no sarama-style non-interruptible-write limitation).
type sdkClient struct {
	client *cloudwatchlogs.Client
}

// newSDKClient builds the production client from the optional region and
// static-credential config keys (aws_cloudwatch.py:26-34); absent keys fall
// back to the SDK's default chain (environment, profile, IMDS).
func newSDKClient(cfg map[string]any) (logsClient, error) {
	region, err := optionalString(cfg, "aws_region")
	if err != nil {
		return nil, err
	}
	accessKey, err := optionalString(cfg, "aws_access_key_id")
	if err != nil {
		return nil, err
	}
	secretKey, err := optionalString(cfg, "aws_secret_access_key")
	if err != nil {
		return nil, err
	}
	var opts []func(*config.LoadOptions) error
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	if accessKey != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
		))
	}
	loaded, err := config.LoadDefaultConfig(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("cloudwatch: load AWS config: %w", err)
	}
	return &sdkClient{client: cloudwatchlogs.NewFromConfig(loaded)}, nil
}

// CreateLogGroup implements logsClient.
func (c *sdkClient) CreateLogGroup(ctx context.Context, group string) error {
	_, err := c.client.CreateLogGroup(ctx, &cloudwatchlogs.CreateLogGroupInput{LogGroupName: aws.String(group)})
	return wrapAPIError(err)
}

// CreateLogStream implements logsClient.
func (c *sdkClient) CreateLogStream(ctx context.Context, group, stream string) error {
	_, err := c.client.CreateLogStream(ctx, &cloudwatchlogs.CreateLogStreamInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
	})
	return wrapAPIError(err)
}

// UploadSequenceToken implements logsClient, paging describe_log_streams until
// the named stream is found (aws_cloudwatch.py:82-90's paginator).
func (c *sdkClient) UploadSequenceToken(ctx context.Context, group, stream string) (string, bool, error) {
	pager := cloudwatchlogs.NewDescribeLogStreamsPaginator(c.client, &cloudwatchlogs.DescribeLogStreamsInput{
		LogGroupName: aws.String(group),
	})
	for pager.HasMorePages() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return "", false, wrapAPIError(err)
		}
		for _, s := range page.LogStreams {
			if aws.ToString(s.LogStreamName) == stream {
				return aws.ToString(s.UploadSequenceToken), true, nil
			}
		}
	}
	return "", false, nil
}

// PutLogEvents implements logsClient. The SDK returns an error for non-2xx
// responses, so a nil error is reported as HTTP 200 (Python's
// ResponseMetadata.HTTPStatusCode check).
func (c *sdkClient) PutLogEvents(ctx context.Context, group, stream, sequenceToken string, events []logEvent) (string, int, error) {
	in := &cloudwatchlogs.PutLogEventsInput{
		LogGroupName:  aws.String(group),
		LogStreamName: aws.String(stream),
		LogEvents:     make([]cwltypes.InputLogEvent, 0, len(events)),
	}
	if sequenceToken != "" {
		in.SequenceToken = aws.String(sequenceToken)
	}
	for _, ev := range events {
		in.LogEvents = append(in.LogEvents, cwltypes.InputLogEvent{
			Timestamp: aws.Int64(ev.Timestamp),
			Message:   aws.String(ev.Message),
		})
	}
	out, err := c.client.PutLogEvents(ctx, in)
	if err != nil {
		return "", 0, wrapAPIError(err)
	}
	return aws.ToString(out.NextSequenceToken), 200, nil
}

// wrapAPIError maps a smithy API error onto apiError so the sender can read
// the botocore Error.Code the same way Python does. Non-API errors pass
// through unchanged.
func wrapAPIError(err error) error {
	if err == nil {
		return nil
	}
	var api smithy.APIError
	if errors.As(err, &api) {
		return &apiError{Code: api.ErrorCode(), Message: api.ErrorMessage()}
	}
	return err
}
