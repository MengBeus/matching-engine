package engine

import "matching-engine/internal/matching"

func cloneCommandExecResult(in *CommandExecResult) *CommandExecResult {
	if in == nil {
		return nil
	}

	return &CommandExecResult{
		Result:    cloneCommandResult(in.Result),
		ErrorCode: in.ErrorCode,
		Err:       in.Err,
	}
}

func cloneCommandResult(in *matching.CommandResult) *matching.CommandResult {
	if in == nil {
		return nil
	}

	out := &matching.CommandResult{
		OrderStatusChanges: append([]matching.OrderStatusChange(nil), in.OrderStatusChanges...),
		Trades:             append([]matching.Trade(nil), in.Trades...),
		Events:             make([]matching.Event, 0, len(in.Events)),
	}

	for _, evt := range in.Events {
		out.Events = append(out.Events, cloneEvent(evt))
	}

	return out
}

func cloneEvent(evt matching.Event) matching.Event {
	switch e := evt.(type) {
	case *matching.OrderAcceptedEvent:
		if e == nil {
			return nil
		}
		cp := *e
		return &cp
	case *matching.OrderMatchedEvent:
		if e == nil {
			return nil
		}
		cp := *e
		return &cp
	case *matching.OrderCanceledEvent:
		if e == nil {
			return nil
		}
		cp := *e
		return &cp
	default:
		return evt
	}
}
