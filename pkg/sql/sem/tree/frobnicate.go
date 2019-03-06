package tree

import (
	"fmt"
)

type Frobnicate struct {
	Mode FrobnicateMode
}

var _ Statement = &Frobnicate{}

type FrobnicateMode int

const (
	FrobnicateModeAll FrobnicateMode = iota
	FrobnicateModeCluster
	FrobnicateModeSession
)

func (node *Frobnicate) StatementType() StatementType { return Ack }
func (node *Frobnicate) StatementTag() string         { return "FROBNICATE" }

func (node *Frobnicate) Format(ctx *FmtCtx) {
	buf := ctx.Buffer
	buf.WriteString("FROBNICATE ")
	switch node.Mode {
	case FrobnicateModeAll:
		buf.WriteString("ALL")
	case FrobnicateModeCluster:
		buf.WriteString("CLUSTER")
	case FrobnicateModeSession:
		buf.WriteString("SESSION")
	default:
		panic(fmt.Errorf("Unknown FROBNICATE mode %v!", node.Mode))
	}
}

func (node *Frobnicate) String() string {
	return AsString(node)
}
