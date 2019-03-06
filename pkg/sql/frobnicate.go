package sql

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

var varOptions = map[string][]string{
	`default_transaction_isolation`: []string{"SNAPSHOT", "SERIALIZABLE"},
	`distsql`:                       []string{"off", "on", "auto", "always"},
}

func randomOption(name string) (string, error) {
	options, ok := varOptions[name]
	if !ok {
		return "", fmt.Errorf("Unknown option %s!", name)
	}

	i := rand.Int() % len(options)
	return options[i], nil
}

func randomName() string {
	length := 10 + rand.Int()%10
	buf := bytes.NewBuffer(make([]byte, 0, length))

	for i := 0; i < length; i++ {
		ch := 'a' + rune(rand.Int()%26)
		buf.WriteRune(ch)
	}

	return buf.String()
}

func randomDatabase(ctx context.Context, p *planner) (string, error) {

	var dbDescs []*sqlbase.DatabaseDescriptor

	err := forEachDatabaseDesc(ctx, p, nil /* all descriptors */, func(dbDesc *sqlbase.DatabaseDescriptor) error {
		dbDescs = append(dbDescs, dbDesc)
		return nil
	})

	if err != nil {
		return "", err
	}

	i := rand.Int() % len(dbDescs)
	return dbDescs[i].GetName(), nil
}

func (p *planner) Frobnicate(ctx context.Context, stmt *tree.Frobnicate) (planNode, error) {
	switch stmt.Mode {
	case tree.FrobnicateModeSession:
		p.randomizeSessionSettings(ctx)
	default:
		return nil, fmt.Errorf("unhandled FROBNICATE mode %v", stmt.Mode)
	}

	return &zeroNode{}, nil
}

func (p *planner) randomizeSessionSettings(ctx context.Context) error {
	db, err := randomDatabase(ctx, p)
	if err != nil {
		return err
	}

	err = varGen["database"].Set(ctx, p.sessionDataMutator, db)
	if err != nil {
		return err
	}

	for option := range varOptions {
		value, err := randomOption(option)
		if err != nil {
			return err
		}

		log.Printf("frobnicating option %s", option)
		varGenOption := varGen[option]
		err = varGenOption.Set(ctx, p.sessionDataMutator, value)
		if err != nil {
			return err
		}
	}

	return varGen["application_name"].Set(ctx, p.sessionDataMutator, randomName())
}
