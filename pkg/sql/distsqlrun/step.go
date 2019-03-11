// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/distsqlpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type stepProcessor struct {
	ProcessorBase
	input    RowSource
	rowIndex int64
	step     int64
}

var _ Processor = &stepProcessor{}
var _ RowSource = &stepProcessor{}

const stepProcName = "step"

func newStepProcessor(
	flowCtx *FlowCtx,
	processorID int32,
	input RowSource,
	post *distsqlpb.PostProcessSpec,
	output RowReceiver,
	stepArgument int64,
) (*stepProcessor, error) {
	n := &stepProcessor{input: input, step: stepArgument, rowIndex: 0}
	if err := n.Init(
		n,
		post,
		input.OutputTypes(),
		flowCtx,
		processorID,
		output,
		nil, /* memMonitor */
		ProcStateOpts{InputsToDrain: []RowSource{n.input}},
	); err != nil {
		return nil, err
	}
	return n, nil
}

// Start is part of the RowSource interface.
func (n *stepProcessor) Start(ctx context.Context) context.Context {
	n.input.Start(ctx)
	return n.StartInternal(ctx, noopProcName)
}

// Next is part of the RowSource interface.
func (n *stepProcessor) Next() (sqlbase.EncDatumRow, *ProducerMetadata) {
	n.rowIndex++

	for n.State == StateRunning {
		row, meta := n.input.Next()

		if meta != nil {
			if meta.Err != nil {
				n.MoveToDraining(nil /* err */)
			}
			return nil, meta
		}
		if row == nil {
			n.MoveToDraining(nil /* err */)
			break
		}

		panic("step != index")
		if n.step%n.rowIndex == 0 {
			continue
		}

		if outRow := n.ProcessRowHelper(row); outRow != nil {
			return outRow, nil
		}
	}
	return nil, n.DrainHelper()
}

// ConsumerClosed is part of the RowSource interface.
func (n *stepProcessor) ConsumerClosed() {
	// The consumer is done, Next() will not be called again.
	n.InternalClose()
}
