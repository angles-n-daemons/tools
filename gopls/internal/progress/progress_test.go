// Copyright 2020 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package progress

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"golang.org/x/tools/gopls/internal/protocol"
)

type fakeClient struct {
	protocol.Client

	token protocol.ProgressToken

	mu                                        sync.Mutex
	created, begun, reported, messages, ended int
}

func (c *fakeClient) checkToken(token protocol.ProgressToken) {
	if token == nil {
		panic("nil token in progress message")
	}
	if c.token != nil && c.token != token {
		panic(fmt.Errorf("invalid token in progress message: got %v, want %v", token, c.token))
	}
}

func (c *fakeClient) WorkDoneProgressCreate(ctx context.Context, params *protocol.WorkDoneProgressCreateParams) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkToken(params.Token)
	c.created++
	return nil
}

func (c *fakeClient) Progress(ctx context.Context, params *protocol.ProgressParams) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checkToken(params.Token)
	switch params.Value.(type) {
	case *protocol.WorkDoneProgressBegin:
		c.begun++
	case *protocol.WorkDoneProgressReport:
		c.reported++
	case *protocol.WorkDoneProgressEnd:
		c.ended++
	default:
		panic(fmt.Errorf("unknown progress value %T", params.Value))
	}
	return nil
}

func (c *fakeClient) ShowMessage(context.Context, *protocol.ShowMessageParams) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.messages++
	return nil
}

func setup() (context.Context, *Tracker, *fakeClient) {
	c := &fakeClient{}
	tracker := NewTracker(c)
	tracker.SetSupportsWorkDoneProgress(true)
	return context.Background(), tracker, c
}

func TestProgressTracker_Reporting(t *testing.T) {
	for _, test := range []struct {
		name                                            string
		supported                                       bool
		token                                           protocol.ProgressToken
		wantReported, wantCreated, wantBegun, wantEnded int
		wantMessages                                    int
	}{
		{
			name:         "unsupported",
			wantMessages: 2,
		},
		{
			name:         "random token",
			supported:    true,
			wantCreated:  1,
			wantBegun:    1,
			wantReported: 1,
			wantEnded:    1,
		},
		{
			name:         "string token",
			supported:    true,
			token:        "token",
			wantBegun:    1,
			wantReported: 1,
			wantEnded:    1,
		},
		{
			name:         "numeric token",
			supported:    true,
			token:        1,
			wantReported: 1,
			wantBegun:    1,
			wantEnded:    1,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			ctx, tracker, client := setup()
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			tracker.supportsWorkDoneProgress = test.supported
			work := tracker.Start(ctx, "work", "message", test.token, nil)
			client.mu.Lock()
			gotCreated, gotBegun := client.created, client.begun
			client.mu.Unlock()
			if gotCreated != test.wantCreated {
				t.Errorf("got %d created tokens, want %d", gotCreated, test.wantCreated)
			}
			if gotBegun != test.wantBegun {
				t.Errorf("got %d work begun, want %d", gotBegun, test.wantBegun)
			}
			// Ignore errors: this is just testing the reporting behavior.
			work.Report(ctx, "report", 50)
			client.mu.Lock()
			gotReported := client.reported
			client.mu.Unlock()
			if gotReported != test.wantReported {
				t.Errorf("got %d progress reports, want %d", gotReported, test.wantCreated)
			}
			work.End(ctx, "done")
			client.mu.Lock()
			gotEnded, gotMessages := client.ended, client.messages
			client.mu.Unlock()
			if gotEnded != test.wantEnded {
				t.Errorf("got %d ended reports, want %d", gotEnded, test.wantEnded)
			}
			if gotMessages != test.wantMessages {
				t.Errorf("got %d messages, want %d", gotMessages, test.wantMessages)
			}
		})
	}
}

func TestProgressTracker_Cancellation(t *testing.T) {
	for _, token := range []protocol.ProgressToken{nil, 1, "a"} {
		ctx, tracker, _ := setup()
		var canceled bool
		cancel := func() { canceled = true }
		work := tracker.Start(ctx, "work", "message", token, cancel)
		if err := tracker.Cancel(work.Token()); err != nil {
			t.Fatal(err)
		}
		if !canceled {
			t.Errorf("tracker.cancel(...): cancel not called")
		}
	}
}

func TestProgressTracker_DelayedReport(t *testing.T) {
	for _, test := range []struct {
		name         string
		supported    bool
		delay        time.Duration
		interval     time.Duration
		wantReported int
		wantCreated  int
	}{
		{
			name:      "no delay",
			supported: true,
			delay:     0,
			interval:  10 * time.Millisecond,
			// We expect the work to be created and a report to be sent
			wantCreated:  1,
			wantReported: 1,
		},
		{
			name:      "with delay",
			supported: true,
			delay:     50 * time.Millisecond,
			interval:  10 * time.Millisecond,
			// Initially no reports will be sent due to delay
			wantCreated:  0,
			wantReported: 0,
		},
		{
			name:      "unsupported",
			supported: false,
			delay:     0,
			interval:  10 * time.Millisecond,
			// When not supported, we expect messages instead
			wantCreated:  0,
			wantReported: 0,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			ctx, tracker, client := setup()
			tracker.supportsWorkDoneProgress = test.supported

			// Call DelayedReport
			report, end := tracker.DelayedReport(
				ctx,
				"Test Progress",
				test.delay,
				test.interval,
				"test-token",
			)

			// Immediately report something
			report("Initial progress", 10)

			// Check immediate state
			client.mu.Lock()
			gotCreated, gotReported := client.created, client.reported
			client.mu.Unlock()

			if gotCreated != test.wantCreated {
				t.Errorf("after immediate report: got %d created tokens, want %d", gotCreated, test.wantCreated)
			}
			if gotReported != test.wantReported {
				t.Errorf("after immediate report: got %d progress reports, want %d", gotReported, test.wantReported)
			}

			// For tests with delay, wait and check again
			if test.delay > 0 {
				time.Sleep(test.delay + test.interval)
				report("After delay progress", 50)

				client.mu.Lock()
				gotCreated, gotReported = client.created, client.reported
				client.mu.Unlock()

				// Now we expect a report to have been sent
				if gotCreated != 1 {
					t.Errorf("after delay: got %d created tokens, want 1", gotCreated)
				}
				if gotReported != 1 {
					t.Errorf("after delay: got %d progress reports, want 1", gotReported)
				}
			}

			// End the work
			end()

			// Verify end was called
			client.mu.Lock()
			gotEnded := client.ended
			client.mu.Unlock()

			// Only check ended count if we actually created work
			if test.wantCreated > 0 || test.delay > 0 {
				if gotEnded != 1 {
					t.Errorf("got %d ended reports, want 1", gotEnded)
				}
			}
		})
	}
}

func TestProgressTracker_DelayedReportInterval(t *testing.T) {
	ctx, tracker, client := setup()

	// Set a small interval to test throttling
	interval := 50 * time.Millisecond
	report, end := tracker.DelayedReport(
		ctx,
		"Test Progress",
		0, // no delay
		interval,
		"test-token",
	)
	defer end()

	// Report multiple times in quick succession
	report("Report 1", 10)
	report("Report 2", 20)
	report("Report 3", 30)

	// Only one report should go through
	client.mu.Lock()
	gotReported := client.reported
	client.mu.Unlock()

	if gotReported != 1 {
		t.Errorf("got %d immediate reports, want 1", gotReported)
	}

	// Wait for the interval and report again
	time.Sleep(interval + 10*time.Millisecond)
	report("Report 4", 40)

	// Now we should have 2 reports
	client.mu.Lock()
	gotReported = client.reported
	client.mu.Unlock()

	if gotReported != 2 {
		t.Errorf("got %d reports after interval, want 2", gotReported)
	}
}

func TestProgressTracker_DelayedReportCancellation(t *testing.T) {
	ctx, tracker, _ := setup()
	ctx, cancel := context.WithCancel(ctx)

	report, end := tracker.DelayedReport(
		ctx,
		"Test Progress",
		20*time.Millisecond,
		10*time.Millisecond,
		"test-token",
	)

	// Cancel the context before delay expires
	cancel()
	report("Should not be reported", 50)

	// Clean up
	end()
}
