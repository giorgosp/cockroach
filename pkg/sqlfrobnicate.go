package sql

import (
    "context"
    "fmt"

    "github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func (p *planner) Frobnicate(ctx context.Context, stmt *tree.Frobnicate) (planNode, error) {
    return nil, fmt.Errorf("We're not quite frobnicating yet...")
}
