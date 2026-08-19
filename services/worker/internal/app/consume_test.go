package app

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

const testTopic = "jobs"

// fetch builds a kgo.Fetches whose records are laid out exactly as given:
// one FetchPartition per entry, in order, so RecordIter walks the partitions
// in that order.
func fetch(parts ...[]*kgo.Record) kgo.Fetches {
	topic := kgo.FetchTopic{Topic: testTopic}
	for _, recs := range parts {
		topic.Partitions = append(topic.Partitions, kgo.FetchPartition{
			Partition: recs[0].Partition,
			Records:   recs,
		})
	}
	return kgo.Fetches{{Topics: []kgo.FetchTopic{topic}}}
}

func recs(partition int32, offsets ...int64) []*kgo.Record {
	var out []*kgo.Record
	for _, o := range offsets {
		out = append(out, &kgo.Record{Topic: testTopic, Partition: partition, Offset: o})
	}
	return out
}

// key identifies a record in the assertions below.
type key struct {
	partition int32
	offset    int64
}

func keyOf(r *kgo.Record) key { return key{r.Partition, r.Offset} }

func TestProcessBatch(t *testing.T) {
	commitErr := errors.New("commit boom")

	tests := []struct {
		name string
		in   kgo.Fetches
		// handleFalse: records Handle refuses; commitFails: records whose commit errors
		handleFalse map[key]bool
		commitFails map[key]bool
		wantHandled []key
		wantCommits []key
		wantStalled bool
	}{
		{
			name:        "all healthy",
			in:          fetch(recs(0, 10, 11), recs(1, 20)),
			wantHandled: []key{{0, 10}, {0, 11}, {1, 20}},
			wantCommits: []key{{0, 10}, {0, 11}, {1, 20}},
		},
		{
			// the regression this test exists for: a stall on partition 0 must
			// not strand the healthy records on partitions 1 and 2
			name:        "stall does not abandon other partitions",
			in:          fetch(recs(0, 10, 11), recs(1, 20, 21), recs(2, 30)),
			handleFalse: map[key]bool{{0, 10}: true},
			wantHandled: []key{{0, 10}, {1, 20}, {1, 21}, {2, 30}},
			wantCommits: []key{{1, 20}, {1, 21}, {2, 30}},
			wantStalled: true,
		},
		{
			// never commit past a stranded offset on its own partition
			name:        "later records on the stalled partition are skipped",
			in:          fetch(recs(0, 10, 11, 12)),
			handleFalse: map[key]bool{{0, 10}: true},
			wantHandled: []key{{0, 10}},
			wantCommits: nil,
			wantStalled: true,
		},
		{
			name:        "commit error stalls its partition only",
			in:          fetch(recs(0, 10, 11), recs(1, 20)),
			commitFails: map[key]bool{{0, 10}: true},
			wantHandled: []key{{0, 10}, {1, 20}},
			wantCommits: []key{{0, 10}, {1, 20}}, // 0/10 was attempted and failed
			wantStalled: true,
		},
		{
			name:        "every partition stalled",
			in:          fetch(recs(0, 10), recs(1, 20)),
			handleFalse: map[key]bool{{0, 10}: true, {1, 20}: true},
			wantHandled: []key{{0, 10}, {1, 20}},
			wantCommits: nil,
			wantStalled: true,
		},
		{
			name: "empty fetch",
			in:   kgo.Fetches{},
		},
	}

	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var handled, commits []key

			handle := func(_ context.Context, r *kgo.Record) bool {
				handled = append(handled, keyOf(r))
				return !tt.handleFalse[keyOf(r)]
			}
			commit := func(_ context.Context, rs ...*kgo.Record) error {
				for _, r := range rs {
					commits = append(commits, keyOf(r))
					if tt.commitFails[keyOf(r)] {
						return commitErr
					}
				}
				return nil
			}

			got := processBatch(context.Background(), log, tt.in, handle, commit)

			if got != tt.wantStalled {
				t.Errorf("stalledAny = %v, want %v", got, tt.wantStalled)
			}
			assertKeys(t, "handled", handled, tt.wantHandled)
			assertKeys(t, "committed", commits, tt.wantCommits)
		})
	}
}

func assertKeys(t *testing.T, what string, got, want []key) {
	t.Helper()
	if len(got) != len(want) {
		t.Errorf("%s = %v, want %v", what, got, want)
		return
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("%s = %v, want %v", what, got, want)
			return
		}
	}
}
