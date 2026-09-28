package workflow

import "fmt"

// ValidateDefinition checks a workflow definition before it is registered:
// tasks must be non-empty, ids unique, and dependencies must form a DAG.
// It returns a *ValidationError with a distinguishable Reason:
//   - ReasonEmptyWorkflow / ReasonDuplicateTask
//   - ReasonMissingDependency (a dep points to a task that does not exist)
//   - ReasonCycleDetected    (the dependency graph contains a cycle)
//
// All checks are performed before scheduling, so a bad definition can never
// produce a partial run.
func ValidateDefinition(def WorkflowDef) error {
	if len(def.Tasks) == 0 {
		return &ValidationError{Reason: ReasonEmptyWorkflow, Msg: "workflow has no tasks"}
	}

	known := make(map[string]bool, len(def.Tasks))
	for _, t := range def.Tasks {
		if t.ID == "" {
			return &ValidationError{Reason: ReasonInvalidDefinition, Msg: "task id must not be empty"}
		}
		if known[t.ID] {
			return &ValidationError{Reason: ReasonDuplicateTask, Msg: "duplicate task id: " + t.ID}
		}
		known[t.ID] = true
	}

	// Missing-dependency check runs before the cycle check so callers can
	// distinguish "dep does not exist" from "dep graph has a cycle".
	for _, t := range def.Tasks {
		seen := map[string]bool{}
		for _, dep := range t.Dependencies {
			if dep == t.ID {
				return &ValidationError{Reason: ReasonCycleDetected, Msg: fmt.Sprintf("task %s depends on itself", t.ID)}
			}
			if !known[dep] {
				return &ValidationError{Reason: ReasonMissingDependency, Msg: fmt.Sprintf("task %s depends on unknown task %s", t.ID, dep)}
			}
			if seen[dep] {
				return &ValidationError{Reason: ReasonInvalidDefinition, Msg: fmt.Sprintf("task %s lists dependency %s more than once", t.ID, dep)}
			}
			seen[dep] = true
		}
	}

	if cycle := findCycle(def); cycle != nil {
		return &ValidationError{
			Reason: ReasonCycleDetected,
			Msg:    fmt.Sprintf("dependency cycle: %v", cycle),
		}
	}
	return nil
}

// findCycle returns the node ids forming a cycle, or nil when the graph is a DAG.
// Iterative DFS with 3-color marking (white/gray/black).
func findCycle(def WorkflowDef) []string {
	const white, gray, black = 0, 1, 2
	color := make(map[string]int, len(def.Tasks))
	deps := make(map[string][]string, len(def.Tasks))
	for _, t := range def.Tasks {
		deps[t.ID] = t.Dependencies
	}

	type frame struct {
		id  string
		idx int
	}

	for _, start := range def.Tasks {
		root := start.ID
		if color[root] != white {
			continue
		}
		stack := []frame{{id: root}}
		color[root] = gray
		path := []string{root}
		for len(stack) > 0 {
			top := &stack[len(stack)-1]
			if top.idx < len(deps[top.id]) {
				next := deps[top.id][top.idx]
				top.idx++
				switch color[next] {
				case white:
					color[next] = gray
					stack = append(stack, frame{id: next})
					path = append(path, next)
				case gray:
					// Back edge -> extract the cycle from path.
					for i, id := range path {
						if id == next {
							return append(path[i:], next)
						}
					}
					return []string{next, next}
				}
			} else {
				color[top.id] = black
				stack = stack[:len(stack)-1]
				path = path[:len(path)-1]
			}
		}
	}
	return nil
}

// TopoOrder returns task ids in dependency order (dependencies before dependents).
// It assumes the definition has already passed ValidateDefinition.
func TopoOrder(def WorkflowDef) ([]string, error) {
	indeg := make(map[string]int, len(def.Tasks))
	children := make(map[string][]string, len(def.Tasks))
	order := make([]string, 0, len(def.Tasks))
	ids := make([]string, 0, len(def.Tasks))
	for _, t := range def.Tasks {
		ids = append(ids, t.ID)
		if _, ok := indeg[t.ID]; !ok {
			indeg[t.ID] = 0
		}
	}
	for _, t := range def.Tasks {
		for _, dep := range t.Dependencies {
			indeg[t.ID]++
			children[dep] = append(children[dep], t.ID)
		}
	}
	// Deterministic queue: process ids in declaration order at each layer.
	queue := make([]string, 0)
	for _, id := range ids {
		if indeg[id] == 0 {
			queue = append(queue, id)
		}
	}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		order = append(order, id)
		var ready []string
		for _, ch := range children[id] {
			indeg[ch]--
			if indeg[ch] == 0 {
				ready = append(ready, ch)
			}
		}
		// preserve declaration order for newly ready nodes
		for _, cand := range ids {
			for _, r := range ready {
				if cand == r {
					queue = append(queue, cand)
				}
			}
		}
	}
	if len(order) != len(def.Tasks) {
		return nil, &ValidationError{Reason: ReasonCycleDetected, Msg: "topological sort failed"}
	}
	return order, nil
}
